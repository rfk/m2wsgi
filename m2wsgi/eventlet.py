
from __future__ import absolute_import 

from m2wsgi import base

import eventlet.hubs
from eventlet.green import zmq, time
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

    def handle_request(self,req):
        eventlet.spawn_n(super(WSGIHandler,self).handle_request,req)


if __name__ == "__main__":
    def application(environ,start_response):
        start_response("200 OK",[("Content-Length","11")])
        yield "hello world"
    s = WSGIHandler(application,"tcp://127.0.0.1:9999")
    s.serve()


