"""

m2wsgi.eventlet:  eventlet-based I/O module for m2wsgi
======================================================


This module provides subclasses of m2wsgi.WSGIHandler and related classes
that are specifically tuned for running under eventlet.  You can import
and use the classes directory from here, or you can select this module
when launching m2wsgi from the command-line::

    python -m m2wsgi --io=eventlet dotted.app.name tcp://127.0.0.1:9999

"""

from __future__ import absolute_import 

from m2wsgi import base

import eventlet.hubs
from eventlet.green import zmq, time, threading
from eventlet.hubs import use_hub
from eventlet import tpool
eventlet.hubs.use_hub("zeromq")


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


