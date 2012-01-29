"""

m2wsgi.io.gevent:  gevent-based I/O module for m2wsgi
=====================================================


This module provides subclasses of m2wsgi.WSGIHandler and related classes
that are specifically tuned for running under gevent.  You can import
and use the classes directory from here, or you can select this module
when launching m2wsgi from the command-line::

    m2wsgi --io=gevent dotted.app.name tcp://127.0.0.1:9999

You will need the gevent_zeromq package from here:

    https://github.com/traviscline/gevent-zeromq

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.


from __future__ import absolute_import 
from m2wsgi.util import fix_absolute_import
fix_absolute_import(__file__)

from m2wsgi.io import base

import gevent
import gevent.monkey
import gevent.event
import gevent.core
import gevent.hub

import gevent_zeromq
from gevent_zeromq import zmq

import zmq.core.poll as zmq_poll


def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module.

    This calls the standard gevent monkey-patching routines.  Don't worry,
    it's not called by default unless you're running from the command line.
    """
    gevent.monkey.patch_all()
    gevent_zeromq.monkey_patch()
    #  Patch signal module for gevent compatability.
    #  Courtesy of http://code.google.com/p/gevent/issues/detail?id=49
    import signal
    _orig_signal = signal.signal
    def gevent_signal_wrapper(signum,*args,**kwds):
        handler = signal.getsignal(signum)
        if callable(handler):
            handler(signum,None)
    def gevent_signal(signum,handler):
        _orig_signal(signum,handler)
        return gevent.hub.signal(signum,gevent_signal_wrapper,signum)
    signal.signal = gevent_signal


#  The BaseConnection recv logic is based on polling, but I can't get
#  gevent polling on multiple sockets to work correctly.
#  Instead, we simulate polling on each socket individually by reading an item
#  and keeping it in a local buffer.
#
#  Ideally I would juse use the _wait_read() method on gevent-zmq sockets,
#  but this seems to cause hangs for me.  Still investigating.

class _Context(zmq._Context):
    def socket(self,socket_type):
        if self.closed:
            raise zmq.ZMQError(zmq.ENOTSUP)
        return _Socket(self,socket_type)
    def term(self):
        #  This seems to be needed to let other greenthreads shut down.
        #  Omit it, and the SIGHUP handler gets  "bad file descriptor" errors.
        gevent.sleep(0.1)
        return super(_Context,self).term()

class _Socket(zmq._Socket):
    def __init__(self,*args,**kwds):
        self._polled_recv = None
        super(_Socket,self).__init__(*args,**kwds)
    #  This blockingly-reads a message from the socket, but stores
    #  it in a buffer rather than returning it.
    def _recv_poll(self,flags=0,copy=True,track=False):
        if self._polled_recv is None:
            self._polled_recv = super(_Socket,self).recv(flags,copy,track)
    #  This uses the buffered result if available, or polls otherwise.
    def recv(self,flags=0,copy=True,track=False):
        v = self._polled_recv
        while v is None:
            self._recv_poll(flags,copy=copy,track=track)
            v = self._polled_recv
        self._polled_recv = None
        return v

zmq.Context = _Context
zmq.Socket = _Socket



class Client(base.Client):
    __doc__ = base.Client.__doc__


class Request(base.Client):
    __doc__ = base.Client.__doc__


class ConnectionBase(base.ConnectionBase):
    __doc__ = base.ConnectionBase.__doc__ + """
    This ConnectionBase subclass is designed for use with gevent.  It uses
    the monkey-patched zmq module from gevent and spawns a number of green
    threads to manage non-blocking IO and interrupts.
    """
    ZMQ_CTX = zmq.Context()

    #  A blocking zmq.core.poll doesn't play nice with gevent.
    #  Instead we read from each socket in a separate greenthread, and keep
    #  the results in a local buffer so they don't get lost.  An interrupt
    #  then just kills all the currently-running threads.
    def __init__(self):
        super(ConnectionBase,self).__init__()
        self.poll_threads = []

    def _poll(self,sockets,timeout=None):
        #  If there's anything available non-blockingly, just use it.
        (ready,_,error) = zmq_poll.select(sockets,[],sockets,timeout=0)
        if ready:
            return ready
        if error:
            return []
        if timeout == 0:
            return []
        #  Spawn a greenthread to poll-recv from each socket.
        ready = []
        threads = []
        res = gevent.event.Event()
        for sock in sockets:
            threads.append(gevent.spawn(self._do_poll,sock,ready,res,timeout))
        self.poll_threads.append((res,threads))
        #  Wait for one of them to return, or for an interrupt.
        try:
            res.wait()
        finally:
            gevent.killall(threads)
            gevent.joinall(threads)
        return ready

    def _do_poll(self,sock,ready,res,timeout):
        if timeout is None:
            sock._recv_poll()
        else:
            with gevent.Timeout(timeout,False):
                sock._recv_poll()
        ready.append(sock)
        if not res.is_set():
            res.set()

    def _interrupt(self):
        for (res,threads) in self.poll_threads:
            gevent.killall(threads)
            if not res.is_set():
                res.set()



class Connection(base.Connection,ConnectionBase):
    __doc__ = base.Connection.__doc__ + """
    This Connection subclass is designed for use with gevent.  It uses the
    monkey-patched zmq module from gevent and spawns a number of green
    threads to manage non-blocking IO and interrupts.
    """


class DispatcherConnection(base.DispatcherConnection,ConnectionBase):
    __doc__ = base.DispatcherConnection.__doc__ + """
    This DispatcherConnection subclass is designed for use with gevent.  It
    uses the monkey-patched zmq module from gevent and spawns a number of
    green threads to manage non-blocking IO and interrupts.
    """


class StreamingUploadFile(base.StreamingUploadFile):
    __doc__ = base.StreamingUploadFile.__doc__ + """
    This StreamingUploadFile subclass is designed for use with gevent.  It
    uses uses gevent.sleep() instead of time.sleep().
    """
    def _wait_for_data(self):
        curpos = self.fileobj.tell()
        cursize = os.fstat(self.fileobj.fileno()).st_size
        while curpos >= cursize:
            gevent.sleep(0.01)
            cursize = os.fstat(self.fileobj.fileno()).st_size


class Handler(base.Handler):
    __doc__ = base.Handler.__doc__ + """
    This Handler subclass is designed for use with gevent.  It spawns a
    a new green thread to handle each incoming request.
    """

    ConnectionClass = Connection

    def __init__(self,*args,**kwds):
        super(Handler,self).__init__(*args,**kwds)
        #  We need to count the number of inflight requests, so the
        #  main thread can wait for them to complete when shutting down.
        self._num_inflight_requests = 0
        self._all_requests_complete = gevent.event.Event()

    def handle_request(self,req):
        self._num_inflight_requests += 1
        if self._num_inflight_requests >= 1:
            self._all_requests_complete.clear()
        @gevent.spawn
        def do_handle_request():
            try:
                self.process_request(req)
            finally:
                self._num_inflight_requests -= 1
                if self._num_inflight_requests == 0:
                    self._all_requests_complete.set()

    def wait_for_completion(self):
        if self._num_inflight_requests > 0:
            self._all_requests_complete.wait()


class WSGIResponder(base.WSGIResponder):
    __doc__ = base.WSGIResponder.__doc__


class WSGIHandler(base.WSGIHandler,Handler):
    __doc__ = base.WSGIHandler.__doc__ + """
    This WSGIHandler subclass is designed for use with gevent.  It spawns a
    a new green thread to handle each incoming request.
    """
    ResponderClass = WSGIResponder
    StreamingUploadClass = StreamingUploadFile


