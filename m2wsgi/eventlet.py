
from __future__ import absolute_import 

from m2wsgi import base

import eventlet.hubs
from eventlet.green import zmq, time
from eventlet.hubs import use_hub
eventlet.hubs.use_hub("zeromq")

class Connection(base.Connection):
    ZMQ_CTX = zmq.Context()


class M2WSGI(base.M2WSGI):
    ConnectionClass = Connection

    def handle_request(self,req):
        eventlet.spawn_n(super(M2WSGI,self).handle_request,req)


if __name__ == "__main__":
    def application(environ,start_response):
        start_response("200 OK",[("Content-Length","11")])
        yield "hello world"
    s = M2WSGI(application,"tcp://127.0.0.1:9999")
    s.run()

