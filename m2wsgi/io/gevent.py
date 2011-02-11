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

import zmq.core.poll as zmq_poll
from gevent_zeromq import zmq


def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module.

    This calls the standard gevent monkey-patching routines.  Don't worry,
    it's not called by default unless you're running from the command line.
    """
    gevent.monkey.patch_all()


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

    #  a blocking zmq.core.poll doesn't play nice with gevent.  We have to
    #  poll based on ZMQ_FD.  Each polled socket gets a persistent gevent
    #  read_event and an Event object to signal on when ready; calls
    #  to interrupt() just signal the events early.
    def __init__(self):
        super(ConnectionBase,self).__init__()
        self.poll_events = {}

    def _poll(self,sockets,timeout=None):
        #  Polling based on ZMQ_FD is edge-triggered, so before we do that
        #  we need to make sure there's no data currently available.
        (ready,_,error) = zmq_poll.select(sockets,[],sockets,timeout=0)
        if ready:
            return ready
        if error:
            return []
        if timeout == 0:
            return []
        #  Now we can do the ZMQ_FD polling.
        #  First make sure each socket has an event and signal set up.
        #  Then spawn a greenthread to wait on each signal.
        threads = []
        res = gevent.event.Event()
        for sock in sockets:
            fd = sock.getsockopt(zmq.FD)
            try:
                (evt,sig) = self.poll_events[fd]
                sig.clear()
            except KeyError:
                sig = gevent.event.Event()
                def on_ready(evt,what,sig=sig):
                    sig.set()
                try:
                    read_event = gevent.hub.get_hub().reactor.read_event
                    evt = read_event(fd,persist=True)
                    evt.add(None,on_ready)
                except AttributeError:
                    evt = gevent.core.read_event(fd,on_ready,persist=True)
                self.poll_events[fd] = (evt,sig)
            def wait_for_signal(sig=sig,res=res,sock=sock):
                sig.wait()
                res.set()
            threads.append(gevent.spawn(wait_for_signal))
        #  Wait for the 'res' event to be triggered by some socket.
        try:
            if timeout is None:
                res.wait()
            else:
                with gevent.Timeout(timeout,False):
                    res.wait()
        finally:
            gevent.killall(threads)
            gevent.joinall(threads)
        #  The peristent poll events just signal us when *something* is
        #  ready, they don't tell us what it was. Nevermind, just ask zmq.
        (ready,_,_) = zmq_poll.select(sockets,[],[],timeout=0)
        return ready

    def _interrupt(self):
        for (evt,sig) in self.poll_events.values():
            sig.set()

    def close(self):
        for (evt,sig) in self.poll_events.values():
            evt.cancel()
            sig.set()
        super(ConnectionBase,self).close()



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


