"""

m2wsgi.gevent:  gevent-based I/O module for m2wsgi
==================================================


This module provides subclasses of m2wsgi.WSGIHandler and related classes
that are specifically tuned for running under gevent.  You can import
and use the classes directory from here, or you can select this module
when launching m2wsgi from the command-line::

    m2wsgi --io=gevent dotted.app.name tcp://127.0.0.1:9999

You will need the gevent_zeromq package from here:

    https://github.com/traviscline/gevent-zeromq

"""

from __future__ import absolute_import 

from m2wsgi import base

import gevent
import gevent.monkey
from gevent_zeromq import zmq


def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module."""
    gevent.monkey.patch_all()

#  Since we need a green Condition object, we must always monkey-patch
monkey_patch()
import threading


class Connection(base.Connection):
    #  Use green version of zmq module.
    ZMQ_CTX = zmq.Context()


class StreamingUploadFile(base.StreamingUploadFile):
    #  Use green sleep function.
    #  Unfortunately no threadpool to call os.fstat
    def _wait_for_data(self):
        curpos = self.fileobj.tell()
        cursize = os.fstat(self.fileobj.fileno()).st_size
        while curpos >= cursize:
            gevent.sleep(0.01)
            cursize = os.fstat(self.fileobj.fileno()).st_size


class WSGIHandler(base.WSGIHandler):
    __doc__ = base.WSGIHandler.__doc__ + """
    This WSGIHandler subclass is designed for use with gevent.  It spawns a
    a new green thread to handle each incoming request.
    """

    ConnectionClass = Connection
    StreamingUploadClass = StreamingUploadFile

    def __init__(self,*args,**kwds):
        super(WSGIHandler,self).__init__(*args,**kwds)
        self._num_inflight_requests = 0
        self._all_requests_complete = threading.Condition()

    def handle_request(self,req):
        @gevent.spawn
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


