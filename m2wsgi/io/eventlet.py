"""

m2wsgi.io.eventlet:  eventlet-based I/O module for m2wsgi
=========================================================


This module provides subclasses of m2wsgi.WSGIHandler and related classes
that are specifically tuned for running under eventlet.  You can import
and use the classes directory from here, or you can select this module
when launching m2wsgi from the command-line::

    m2wsgi --io=eventlet dotted.app.name tcp://127.0.0.1:9999

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.


from __future__ import absolute_import 
from m2wsgi.util import fix_absolute_import
fix_absolute_import(__file__)

from m2wsgi.io import base

import zmq.core.poll as zmq_poll

import eventlet.hubs
from eventlet.green import zmq, time
from eventlet.timeout import Timeout
from eventlet.event import Event

from greenlet import GreenletExit


#  Older eventlet versions have buggy support for non-blocking zmq requests:
#     https://bitbucket.org/which_linden/eventlet/issue/76/
#
if map(int,eventlet.__version__.split(".")) <= [0,9,14]:
    raise ImportError("requires eventlet >= 0.9.15")


def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module.

    This calls the standard eventlet monkey-patching routines.  Don't worry,
    it's not called by default unless you're running from the command line.
    """
    eventlet.monkey_patch()


class Client(base.Client):
    __doc__ = base.Client.__doc__


class Request(base.Client):
    __doc__ = base.Client.__doc__


class ConnectionBase(base.ConnectionBase):
    __doc__ = base.ConnectionBase.__doc__ + """
    This ConnectionBase subclass is designed for use with eventlet.  It uses
    the monkey-patched zmq module from eventlet and spawns a number of
    greenthreads to manage non-blocking IO and interrupts.
    """
    ZMQ_CTX = zmq.Context()

    #  Since zmq.core.poll doesn't play nice with eventlet, we use a
    #  greenthread to implement interrupts.  Each call to _poll() spawns 
    #  a new greenthread for each socket and waits on them; calls to
    #  interrupt() kill the pending threads.
    def __init__(self):
        super(ConnectionBase,self).__init__()
        self.poll_threads = []

    def _poll(self,sockets,timeout=None):
        #  Don't bother trampolining if there's data available immediately.
        #  This also avoids calling into the eventlet hub with a timeout of
        #  zero, which doesn't work right (it still switches the greenthread)
        (r,_,_) = zmq_poll.select(sockets,[],[],timeout=0)
        if r:
            return r
        if timeout == 0:
            return []
        #  Looks like we'll have to block :-(
        ready = []
        threads = []
        res = Event()
        for sock in sockets:
            threads.append(eventlet.spawn(self._do_poll,sock,ready,res,timeout))
        self.poll_threads.append((res,threads))
        try:
            res.wait()
        finally:
            self.poll_threads.remove((res,threads))
            for t in threads:
                t.kill()
                try:
                    t.wait()
                except GreenletExit:
                    pass
        return ready

    def _do_poll(self,sock,ready,res,timeout):
        fd = sock.getsockopt(zmq.FD)
        try:
            zmq.trampoline(fd,read=True,timeout=timeout)
        except Timeout:
            pass
        else:
            ready.append(sock)
            if not res.ready():
                res.send()

    def _interrupt(self):
        for (res,threads) in self.poll_threads:
            if not res.ready():
                res.send()
            for t in threads:
                t.kill()


class Connection(base.Connection,ConnectionBase):
    __doc__ = base.Connection.__doc__ + """
    This Connection subclass is designed for use with eventlet.  It uses
    the monkey-patched zmq module from eventlet and spawns a number of
    green threads to manage non-blocking IO and interrupts.
    """


class DispatcherConnection(base.DispatcherConnection,ConnectionBase):
    __doc__ = base.DispatcherConnection.__doc__ + """
    This DispatcherConnection subclass is designed for use with eventlet.  It
    uses the monkey-patched zmq module from eventlet and spawns a number of
    green threads to manage non-blocking IO and interrupts.
    """


class StreamingUploadFile(base.StreamingUploadFile):
    __doc__ = base.StreamingUploadFile.__doc__ + """
    This StreamingUploadFile subclass is designed for use with eventlet.  It
    uses the monkey-patched time module from eventlet when sleeping.
    """
    def _wait_for_data(self):
        curpos = self.fileobj.tell()
        cursize = os.fstat(self.fileobj.fileno()).st_size
        while curpos >= cursize:
            time.sleep(0.01)
            cursize = os.fstat(self.fileobj.fileno()).st_size


class Handler(base.Handler):
    __doc__ = base.Handler.__doc__ + """
    This Handler subclass is designed for use with eventlet.  It spawns a
    a new green thread to handle each incoming request.
    """
    ConnectionClass = Connection

    def __init__(self,*args,**kwds):
        super(Handler,self).__init__(*args,**kwds)
        #  We need to count the number of inflight requests, so the
        #  main thread can wait for them to complete when shutting down.
        self._num_inflight_requests = 0
        self._all_requests_complete = None

    def handle_request(self,req):
        self._num_inflight_requests += 1
        if self._num_inflight_requests == 1:
            self._all_requests_complete = Event()
        @eventlet.spawn_n
        def do_handle_request():
            try:
                self.process_request(req)
            finally:
                self._num_inflight_requests -= 1
                if self._num_inflight_requests == 0:
                    self._all_requests_complete.send()
                    self._all_requests_complete = None

    def wait_for_completion(self):
        if self._num_inflight_requests > 0:
            self._all_requests_complete.wait()


class WSGIResponder(base.WSGIResponder):
    __doc__ = base.WSGIResponder.__doc__


class WSGIHandler(base.WSGIHandler,Handler):
    __doc__ = base.WSGIHandler.__doc__ + """
    This WSGIHandler subclass is designed for use with eventlet.  It spawns a
    a new green thread to handle each incoming request.
    """
    ResponderClass = WSGIResponder
    StreamingUploadClass = StreamingUploadFile

