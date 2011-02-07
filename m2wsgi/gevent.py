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
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.


from __future__ import absolute_import 
from m2wsgi.util import fix_absolute_import
fix_absolute_import(__file__)

from m2wsgi import base

import gevent
import gevent.monkey
import gevent.event
from gevent_zeromq import zmq


def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module.

    This calls the standard eventlet monkey-patching routines.  Don't worry,
    it's not called by default unless you're running from the command line.
    """
    gevent.monkey.patch_all()


class Connection(base.Connection):
    __doc__ = base.Handler.__doc__ + """
    This Connection subclass is designed for use with gevent.  It uses the
    monkey-patched zmq module from gevent and spawns a number of greenthreads
    to manage non-blocking IO and interrupts.
    """
    #  Use green version of zmq module.
    ZMQ_CTX = zmq.Context()

    #  Since zmq.core.poll doesn't play nice with gevent, we use a
    #  greenthread to implement interrupts.  Each call to _recv() spawns 
    #  a new greenthread and waits on it; calls to interrupt() kill the
    #  pending recv threads.
    def _more_init(self):
        self.recv_threads = []

    class _Interrupted(Exception):
        """Exception raised when a recv() is interrupted."""
        pass

    def _recv(self,timeout=None):
        rt = gevent.spawn(self._do_recv,timeout=timeout)
        self.recv_threads.append(rt)
        try:
            return rt.get()
        except self._Interrupted:
            return None
        finally:
            self.recv_threads.remove(rt)

    def _do_recv(self,timeout=None):
        if timeout is None:
            return self.send_sock.recv()
        elif timeout != 0:
            with gevent.Timeout(timeout,False):
                return self.send_sock.recv()
        else:
            try:
                return self.send_sock.recv(zmq.NOBLOCK)
            except zmq.ZMQError, e:
                if e.errno != zmq.EAGAIN:
                    if not self._has_shutdown or e.errno != zmq.ENOTSUP:
                        raise
                return None

    def interrupt(self):
        for rt in self.recv_threads:
            rt.kill(self._Interrupted)

    def _close(self):
        pass



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


class WSGIHandler(base.WSGIHandler,Handler):
    __doc__ = base.WSGIHandler.__doc__ + """
    This WSGIHandler subclass is designed for use with gevent.  It spawns a
    a new green thread to handle each incoming request.
    """
    StreamingUploadClass = StreamingUploadFile


if __name__ == "__main__":
    def application(environ,start_response):
        start_response("200 OK",[("Content-Length","11")])
        yield "hello world"
    s = WSGIHandler(application,"tcp://127.0.0.1:9999")
    s.serve()


