"""

m2wsgi.eventlet:  eventlet-based I/O module for m2wsgi
======================================================


This module provides subclasses of m2wsgi.WSGIHandler and related classes
that are specifically tuned for running under eventlet.  You can import
and use the classes directory from here, or you can select this module
when launching m2wsgi from the command-line::

    m2wsgi --io=eventlet dotted.app.name tcp://127.0.0.1:9999

"""

from __future__ import absolute_import 
from m2wsgi.util import fix_absolute_import
fix_absolute_import(__file__)


from m2wsgi import base

import eventlet.hubs
from eventlet.green import zmq, time, threading
from eventlet.hubs import use_hub
from eventlet import tpool
eventlet.hubs.use_hub("zeromq")

#  Eventlet has buggy support for non-blocking zmq requests:
#
#     https://bitbucket.org/which_linden/eventlet/issue/76/
#
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
    """Hook to monkey-patch the interpreter for this IO module."""
    eventlet.monkey_patch()


class Connection(base.Connection):
    #  Use green version of zmq module.
    ZMQ_CTX = zmq.Context()


class StreamingUploadFile(base.StreamingUploadFile):
    #  Use green version of time module.
    #  Send os.fstat calls into a threadpool.
    def _wait_for_data(self):
        curpos = self.fileobj.tell()
        cursize = tpool.execute(os.fstat,self.fileobj.fileno()).st_size
        while curpos >= cursize:
            time.sleep(0.01)
            cursize = tpool.execute(os.fstat,self.fileobj.fileno()).st_size


class WSGIHandler(base.WSGIHandler):
    __doc__ = base.WSGIHandler.__doc__ + """
    This WSGIHandler subclass is designed for use with eventlet.  It spawns a
    a new green thread to handle each incoming request.
    """

    ConnectionClass = Connection
    StreamingUploadClass = StreamingUploadFile

    def __init__(self,*args,**kwds):
        super(WSGIHandler,self).__init__(*args,**kwds)
        self._num_inflight_requests = 0
        self._all_requests_complete = threading.Condition()

    def handle_request(self,req):
        @eventlet.spawn_n
        def do_handle_request():
            with self._all_requests_complete:
                self._num_inflight_requests += 1
            super(WSGIHandler,self).handle_request(req)
            with self._all_requests_complete:
                self._num_inflight_requests -= 1
                if self._num_inflight_requests == 0:
                    self._all_requests_complete.notifyAll()

    def wait_for_completion(self):
        with self._all_requests_complete:
            if self._num_inflight_requests > 0:
                self._all_requests_complete.wait()


if __name__ == "__main__":
    def application(environ,start_response):
        start_response("200 OK",[("Content-Length","11")])
        yield "hello world"
    s = WSGIHandler(application,"tcp://127.0.0.1:9999")
    s.serve()


