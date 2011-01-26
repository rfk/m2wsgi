
from __future__ import absolute_import 

from m2wsgi import base

import eventlet.hubs
from eventlet.green import zmq, time
from eventlet.hubs import use_hub
eventlet.hubs.use_hub("zeromq")


class Connection(base.Connection):
    ZMQ_CTX = zmq.Context()


def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module."""
    eventlet.monkey_patch()


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


