"""

m2wsgi.eventlet:  eventlet-based I/O module for m2wsgi
======================================================


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


from m2wsgi import base

import eventlet.hubs
from eventlet.green import zmq, time
from eventlet.timeout import Timeout
from eventlet.event import Event
from eventlet.hubs import use_hub
eventlet.hubs.use_hub("zeromq")

#  Older eventlet versions have buggy support for non-blocking zmq requests:
#
#     https://bitbucket.org/which_linden/eventlet/issue/76/
#
if map(int,eventlet.__version__.split(".")) <= [0,9,14]:
    _BaseSocket = zmq.__zmq__.Socket
    class _Socket(_BaseSocket):
        def _send_message(self,msg,flags=0):
            if flags & zmq.NOBLOCK:
                super(_Socket,self)._send_message(msg,flags)
                return
            flags |= zmq.NOBLOCK
            while True:
                try:
                    super(_Socket,self)._send_message(msg,flags)
                    return
                except zmq.ZMQError, e:
                    if e.errno != zmq.EAGAIN:
                        raise
                zmq.trampoline(self,write=True)
        def _send_copy(self,msg,flags=0):
            if flags & zmq.NOBLOCK:
                super(_Socket,self)._send_copy(msg,flags)
                return
            flags |= zmq.NOBLOCK
            while True:
                try:
                    super(_Socket,self)._send_copy(msg,flags)
                    return
                except zmq.ZMQError, e:
                    if e.errno != zmq.EAGAIN:
                        raise
                zmq.trampoline(self,write=True)
        def _recv_message(self,flags=0,track=False):
            if flags & zmq.NOBLOCK:
                return super(_Socket,self)._recv_message(flags,track)
            flags |= zmq.NOBLOCK
            while True:
                try:
                    return super(_Socket,self)._recv_message(flags,track)
                except zmq.ZMQError, e:
                    if e.errno != zmq.EAGAIN:
                        raise
                zmq.trampoline(self,read=True)
        def _recv_copy(self,flags=0):
            if flags & zmq.NOBLOCK:
                return super(_Socket,self)._recv_copy(flags)
            flags |= zmq.NOBLOCK
            while True:
                try:
                    return super(_Socket,self)._recv_copy(flags)
                except zmq.ZMQError, e:
                    if e.errno != zmq.EAGAIN:
                        raise
                zmq.trampoline(self,read=True)
    zmq.Socket = _Socket


def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module.

    This calls the standard eventlet monkey-patching routines.  Don't worry,
    it's not called by default unless you're running from the command line.
    """
    eventlet.monkey_patch()


class Connection(base.Connection):
    __doc__ = base.Connection.__doc__ + """
    This Connection subclass is designed for use with eventlet.  It uses the
    monkey-patched zmq module from eventlet and spawns a number of greenthreads
    to manage non-blocking IO and interrupts.
    """
    #  Use green version of zmq module.
    ZMQ_CTX = zmq.Context()

    #  Since zmq.core.poll doesn't play nice with eventlet, we use a
    #  greenthread to implement interrupts.  Each call to _recv() spawns 
    #  a new greenthread and waits on it; calls to interrupt() kill the
    #  pending recv threads.
    def _more_init(self):
        self.recv_threads = []

    class _Interrupted(Exception):
        """Exception raised when a recv() is interrupted."""
        pass

    def _recv(self,timeout=None):
        rt = eventlet.spawn(self._do_recv,timeout=timeout)
        return rt.wait()
        self.recv_threads.append(rt)
        try:
            return rt.wait()
        except self._Interrupted:
            return None
        finally:
            self.recv_threads.remove(rt)

    def _do_recv(self,timeout=None):
        try:
            if timeout is None:
                return self.send_sock.recv()
            elif timeout != 0:
                with Timeout(timeout,False):
                    return self.send_sock.recv()
            else:
                return self.send_sock.recv(zmq.NOBLOCK)
        except zmq.ZMQError, e:
            if e.errno != zmq.EAGAIN:
                if not self._has_shutdown:
                    raise
                if e.errno not in (zmq.ENOTSUP,zmq.EFAULT,):
                    raise
            return None

    def interrupt(self):
        for rt in self.recv_threads:
            rt.kill(self._Interrupted)

    def _close(self):
        pass


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


class WSGIHandler(base.WSGIHandler,Handler):
    __doc__ = base.WSGIHandler.__doc__ + """
    This WSGIHandler subclass is designed for use with eventlet.  It spawns a
    a new green thread to handle each incoming request.
    """
    StreamingUploadClass = StreamingUploadFile


if __name__ == "__main__":
    def application(environ,start_response):
        start_response("200 OK",[("Content-Length","11")])
        yield "hello world"
    s = WSGIHandler(application,"tcp://127.0.0.1:9999")
    s.serve()


