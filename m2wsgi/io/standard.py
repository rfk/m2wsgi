"""

m2wsgi.io.standard:  standard I/O module for m2wsgi
===================================================


This module contains the basic classes for implementing handlers with m2wsgi.
They use standard blocking I/O and are designed to work with normal python
threads. If you don't explicitly select a different IO module, you'll get the
classes from in here.  We have the following classes:

    :Connection:     represents the connection from your handler to Mongrel2,
                     through which you can read requests and send responses.

    :Client:         represents a client connected to the server, to whom you
                     can send data at any time.

    :Request:        represents a client request to which you can send
                     response data to at any time.

    :Handler:        a base class for implementing handlers, with nothing
                     WSGI-specific in it.

    :WSGIHandler:    a handler subclass specifically running a WSGI app.


    :WSGIResponder:  a class for managing the stateful aspects of a WSGI
                     response (status line, write callback, etc).


"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.

from __future__ import absolute_import 
from m2wsgi.util import fix_absolute_import
fix_absolute_import(__file__)

import os
import time

import zmq.core.poll

from m2wsgi.io import base



def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module.

    Since this module is designed for standard blocking I/O, there's actually
    no need to monkey-patch anything.  But we provide a dummy function anyway.
    """
    pass


class Client(base.Client):
    __doc__ = base.Client.__doc__


class Request(base.Client):
    __doc__ = base.Client.__doc__


class ConnectionBase(base.ConnectionBase):
    __doc__ = base.ConnectionBase.__doc__ 

    def __init__(self):
        #  We use an anonymous pipe to interrupt blocking receives.
        #  The _poll() always polls intr_pipe_r.
        (self.intr_pipe_r,self.intr_pipe_w) = os.pipe()
        super(ConnectionBase,self).__init__()

    def _poll(self,sockets,timeout=None):
        if timeout != 0:
            sockets = sockets + [self.intr_pipe_r]
        (ready,_,_) = zmq.core.poll.select(sockets,[],[],timeout=timeout)
        if self.intr_pipe_r in ready:
            os.read(self.intr_pipe_r,1)
            ready.remove(self.intr_pipe_r)
        return ready

    def _interrupt(self):
        #  First make sure there are no old interrupts in the pipe.
        socks = [self.intr_pipe_r]
        (ready,_,_) = zmq.core.poll.select(socks,[],[],timeout=0)
        while ready:
            os.read(self.intr_pipe_r,1)
            (ready,_,_) = zmq.core.poll.select(socks,[],[],timeout=0)
        #  Now write to the interrupt pipe to trigger it.
        os.write(self.intr_pipe_w,"X")

    def close(self):
        super(ConnectionBase,self).close()
        os.close(self.intr_pipe_r)
        self.intr_pipe_r = None
        os.close(self.intr_pipe_w)
        self.intr_pipe_w = None


class Connection(base.Connection,ConnectionBase):
    __doc__ = base.Connection.__doc__ 


class DispatcherConnection(base.DispatcherConnection,ConnectionBase):
    __doc__ = base.DispatcherConnection.__doc__


class StreamingUploadFile(base.StreamingUploadFile):
    __doc__ = base.StreamingUploadFile.__doc__
    def _wait_for_data(self):
        """Wait for more data to be available from this upload.

        The standard implementation simply does a sleep loop until the
        file grows past its current position.  Eventually we could try
        using file notifications to detect change.
        """
        curpos = self.fileobj.tell()
        cursize = os.fstat(self.fileobj.fileno()).st_size
        while curpos >= cursize:
            time.sleep(0.01)
            cursize = os.fstat(self.fileobj.fileno()).st_size


class Handler(base.Handler):
    __doc__ = base.Handler.__doc__
    ConnectionClass = Connection


class WSGIResponder(base.WSGIResponder):
    __doc__ = base.WSGIResponder.__doc__


class WSGIHandler(base.WSGIHandler,Handler):
    __doc__ = base.WSGIHandler.__doc__
    ResponderClass = WSGIResponder
    StreamingUploadClass = StreamingUploadFile

