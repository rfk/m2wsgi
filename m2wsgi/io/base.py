"""

m2wsgi.bio.base:  abstract base I/O classes for m2wsgi
======================================================

This module contains the base implementations of various m2wsgi classes.
Many of them are abstract, so don't use this module directly.
We have the following classes:

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

import os
import sys
import re
import json
import time
import traceback
from collections import deque
from cStringIO import StringIO
from email.utils import formatdate as rfc822_format_date

import zmq

from m2wsgi.util import pop_tnetstring, unquote_path, unquote
from m2wsgi.util import InputStream, force_ascii


class Client(object):
    """Mongrel2 client connection object.

    Instances of this object represent a client connected to the Mongrel2
    server.  They encapsulate the server id and client connection id, and
    provide some handy methods to send data to the client.
    """

    def __init__(self,connection,server_id,client_id):
        self.connection = connection
        self.server_id = server_id
        self.client_id = client_id

    def __hash__(self):
        return hash((self.connection,self.server_id,self.client_id))

    def __eq__(self,other):
        if self.connection != other.connection:
            return False
        if self.server_id != other.server_id:
            return False
        if self.client_id != other.client_id:
            return False
        return True

    def __ne__(self,other):
        return not (self == other)

    def send(self,data):
        """Send some data to this client."""
        self.connection.send(self,data)

    def disconnect(self):
        """Terminate this client connection."""
        self.connection.send(self,"")


class Request(object):
    """Mongrel2 request object.

    This is a simple container object for the data in a Mongrel2 request.
    It can be used to poke around in the headers or body of the request,
    and to send replies to the request.
    """

    def __init__(self,client,path,headers,body):
        self.client = client
        self.path = path
        self.headers = headers
        self.body = body

    def __eq__(self,other):
        if self.client != other.client:
            return False
        if self.path != other.path:
            return False
        if self.headers != other.headers:
            return False
        if self.body != other.body:
            return False
        return True

    def __ne__(self,other):
        return not (self == other)

    @classmethod
    def parse(cls,client,msg):
        """Parse a Request out of a (partial) Mongrel2 message.

        This is an alternate constructor for the class, which takes the
        client object and the leftover parts of the Mongrel2 message and
        constructs a Request from that.
        """
        (path,rest) = msg.split(" ",1)
        (headers,rest) = pop_tnetstring(rest)
        (body,_) = pop_tnetstring(rest)
        if isinstance(headers,basestring):
            headers = force_ascii(json.loads(headers))
        return cls(client,path,headers,body)

    def respond(self,data):
        """Send some response data to the issuer of this request."""
        self.client.send(data)

    def disconnect(self):
        """Terminate the connection associated with this request."""
        self.client.disconnect()


class SocketSpec(object):
    """A specification for creating a socket.

    Instances of this class represent the information needed when specifying
    a socket, which may include:

        * socket address (zmq transport protocol and endpoint)
        * socket identity
        * whether to bind() or connect()

    To allow easy use in command-line applications, each SocketSpec has a
    canonical representation as a string in the following format::

        [protocol]://[identity]@[endpoint]#[connect or bind]

    For example, an anonymous socket binding to tcp 127.0.0.1 on port 999
    would be specified like this:

        tcp://127.0.0.1:999#bind

    While an ipc socket with identity "RILEY" connecting to path /my/sock
    would be specified like this:

        ipc://RILEY@/my/sock#connect

    Note that we don't use urlparse for this because it's got its own ideas
    about what protocols support for features of a URL, and they don't agree
    with our needs.
    """

    SPEC_RE = re.compile("""^
                            (?P<protocol>[a-zA-Z]+)
                            ://
                            ((?P<identity>[^@]*)@)?
                            (?P<endpoint>[^\#]+)
                            (\#(?P<mode>[a-zA-Z]*))?
                            $
                         """,re.X)

    def __init__(self,address,identity=None,bind=False):
        self.address = address
        self.identity = identity
        self.bind = bind

    @classmethod
    def parse(cls,str,default_bind=False):
        m = cls.SPEC_RE.match(str)
        if not m:
            raise ValueError("invalid socket spec: %s" % (str,))
        address = m.group("protocol") + "://" + m.group("endpoint")
        identity = m.group("identity")
        bind = m.group("mode")
        if not bind:
            bind = default_bind
        else:
            bind = bind.lower()
            if bind == "bind":
                bind = True
            elif bind == "connect":
                bind = False
            else:
                bind = default_bind
        return cls(address,identity,bind)

    def __str__(self):
        s = self.address
        if self.identity:
             (tr,ep) = self.address.split("://",1)
             s = "%s://%s@%s" % (tr,self.identity,ep,)
        if self.bind:
            s += "#bind"
        return s
 

class ConnectionBase(object):
    """Base class for Mongrel2 connection objects.

    Instances of ConnectionBase represent a handler's connection to the main
    Mongrel2 server(s).  They are used to receive requests and send responses.

    You generally don't want a direct instance of ConnectionBase; use one of
    the subclasses such as Connection or DispatcherConnection.  You might
    also like to make your own subclass by overriding the following methods:

        _poll:       block until sockets are ready for reading
        _interrupt:  interrupt any blocking calls to poll()
        _recv:       receive a request message from the server
        _send:       send response data to the server
        shutdown:    cleanly disconnect from the server

    """

    ZMQ_CTX = zmq.Context()

    ClientClass = Client
    RequestClass = Request

    def __init__(self):
        self.recv_buffer = deque()
        self._has_shutdown = False

    @classmethod
    def makesocket(cls,type,spec=None,bind=False):
        """Make a new socket of given type, according to the given spec.

        This method is used for easy creation of sockets from a string
        description.  It's used internally by ConnectionBase subclasses
        if they are given a string instead of a socket, and you can use
        it externally to create sockets for them.
        """
        sock = cls.ZMQ_CTX.socket(type)
        if spec is not None:
            if isinstance(spec,basestring):
                spec = SocketSpec.parse(spec,default_bind=bind)
            if spec.identity:
                sock.setsockopt(zmq.IDENTITY,spec.identity)
            if spec.address:
                if spec.bind:
                    sock.bind(spec.address)
                else:
                    sock.connect(spec.address)
        return sock

    @classmethod
    def copysockspec(cls,in_spec,relport):
        """Copy the given socket spec, adjust port.

        This is useful for filling in defaults, e.g. inferring the recv
        spec from the send spec.
        """
        if not isinstance(in_spec,basestring):
            return None
        try:
            (in_head,in_port) = in_spec.rsplit(":",1)
            if "#" not in in_port:
                in_tail = None
            else:
                (in_port,in_tail) = in_port.split("#",1)
            out_port = str(int(in_port) + relport)
            out_spec = in_head + ":" + out_port
            if in_tail is not None:
                out_spec += "#" + in_tail
            return out_spec
        except (ValueError,TypeError,IndexError):
            return None

    def recv(self,timeout=None):
        """Receive a request from the send socket.

        This method receives a request from the send socket, parses it into
        a Request object and returns it.
 
        You may specify the keyword argument 'timeout' to specify the max
        number of seconds to block.  Zero means non-blocking and None means
        blocking indefinitely.  If the timeout expires without a request
        being available, None is returned.
        """
        if self.recv_buffer:
            msg = self.recv_buffer.popleft()
        else:
            try:
                msg = self._recv(timeout=timeout)
            except zmq.ZMQError, e:
                if e.errno != zmq.EAGAIN:
                    if not self._has_shutdown:
                        raise
                    if e.errno not in (zmq.ENOTSUP,zmq.EFAULT,):
                        raise
                return None
            else:
                if msg is None:
                    return None
        #  Parse out the request object and return.
        (server_id,client_id,rest) = msg.split(' ', 2)
        client = self.ClientClass(self,server_id,client_id)
        return self.RequestClass.parse(client,rest)

    def _recv(self,timeout=None):
        """Internal method for receving a message.

        This method must be implemented by subclasses.  It should retrieve
        a request message and return it, or return None if it times out.
        It's OK for it to raise EAGAIN, this will be captured up the chain.
        """
        raise NotImplementedError

    def _send(self,server_id,client_ids,data):
        """Internal method to send out response data."""
        raise NotImplementedError

    def _poll(self,sockets,timeout=None):
        """Poll the given sockets, waiting for one to be ready for reading.

        This method must be implemented by subclasses.  It should block until
        one of the sockets and/or file descriptors in 'sockets' is ready for
        reading, until the optional timeout has expired, or until someone calls
        the interrupt() method.

        Typically this would be a wrapper around zmq.core.poll.select, with
        whatever logic is necessary to allow interrupts from another thread.
        """
        raise NotImplementedError

    def _interrupt(self):
        """Internal method for interrupting a poll.

        This method must be implemented by subclasses.  It should cause any
        blocking calls to _poll() to return immediately.
        """
        raise NotImplementedError

    def send(self,client,data):
        """Send a response to the specified client."""
        self._send(client.server_id,client.client_id,data)

    def deliver(self,clients,data):
        """Send the same response to multiple clients.

        This more efficient than sending to them individually as it can
        batch up the replies.
        """
        #  Batch up the clients by their server id.
        #  If we get more than 100 to the same server, send them
        #  immediately.  The rest we send in a final iteration at the end.
        cids_by_sid = {}
        for client in clients:
            (sid,cid) = (client.server_id,client.client_id)
            if sid not in cids_by_sid:
                cids_by_sid[sid] = [cid]
            else:
                cids = cids_by_sid[sid]
                cids.append(cid)
                if len(cids) == 100:
                    self._send(sid," ".join(cids),data)
                    del cids_by_sid[sid]
        for (sid,cids) in cids_by_sid.itervalues():
            self._send(sid," ".join(cids),data)

    def interrupt(self):
        """Interrupt any blocking recv() calls on this connection.

        Calling this method will cause any blocking recv() calls to be
        interrupted and immediately return None.  You might like to call
        this if you're trying to shut down a handler from another thread.
        """
        if self._has_shutdown:
            return
        self._interrupt()

    def shutdown(self,timeout=None):
        """Shut down the connection.

        This indicates that no more requests should be received by the
        handler, but it is willing to process any that have already been
        transmitted.  Use it for graceful termination of handlers.

        After shutdown, you may only call recv() with timeout=0.
        """
        self._has_shutdown = True

    def close(self):
        """Close the connection."""
        self._has_shutdown = True



class Connection(ConnectionBase):
    """A standard PULL/PUB connection to Mongrel2.

    This class represents the standard handler connection to Mongrel2.
    It gets requests pushed to it via a PULL socket and sends response data
    via a PUB socket.
    """

    def __init__(self,send_sock,recv_sock=None):
        if recv_sock is None:
            recv_sock = self.copysockspec(send_sock,-1)
            if recv_sock is None:
                raise ValueError("could not infer recv socket spec")
        if isinstance(send_sock,basestring):
            send_sock = self.makesocket(zmq.PULL,send_sock)
        self.send_sock = send_sock
        if isinstance(recv_sock,basestring):
            recv_sock = self.makesocket(zmq.PUB,recv_sock)
        self.recv_sock = recv_sock
        super(Connection,self).__init__()

    def _recv(self,timeout=None):
        """Internal method for receving a message."""
        ready = self._poll([self.send_sock],timeout=timeout)
        if self.send_sock in ready:
            return self.send_sock.recv(zmq.NOBLOCK)

    def _send(self,server_id,client_ids,data):
        """Internal method to send out response data."""
        msg = "%s %d:%s, %s" % (server_id,len(client_ids),client_ids,data)
        self.recv_sock.send(msg)

    def shutdown(self,timeout=None):
        """Shut down the connection.

        This indicates that no more requests should be received by the
        handler, but it is willing to process any that have already been
        transmitted.  Use it for graceful termination of handlers.

        After shutdown, you may only call recv() with timeout=0.

        For the standard PULL socket, a clean shutdown is not possible
        as zmq has no API for it.  What we do is quickly ready anything
        that's pending for delivery then close the socket.  This leaves
        a slight race condition that a request will be pushed to us and
        then lost.
        """
        msg = self._recv(timeout=0)
        while msg is not None:
            self.recv_buffer.append(msg)
            msg = self._recv(timeout=0)
        self.send_sock.close()
        super(Connection,self).shutdown(timeout)

    def close(self):
        """Close the connection."""
        self.send_sock.close()
        self.recv_sock.close()
        super(Connection,self).close()


class DispatcherConnection(ConnectionBase):
    """A connection to Mongrel2 via a m2wsgi Dispatcher device.

    This class is designed to work with the m2wsgi Dispatcher device.  It
    gets requests and sends replies over a single XREQ socket, and also 
    listens for heartbeat pings on a SUB socket.
    """

    def __init__(self,disp_sock,ping_sock=None):
        super(DispatcherConnection,self).__init__()
        if ping_sock is None:
            ping_sock = self.copysockspec(disp_sock,1)
            if ping_sock is None:
                raise ValueError("could not infer ping socket spec")
        self._shutting_down = False
        if isinstance(disp_sock,basestring):
            disp_sock = self.makesocket(zmq.XREQ,disp_sock)
        self.disp_sock = disp_sock
        if isinstance(ping_sock,basestring):
            ping_sock = self.makesocket(zmq.SUB,ping_sock)
        ping_sock.setsockopt(zmq.SUBSCRIBE,"")
        self.ping_sock = ping_sock
        #  Introduce ourselves to the dispatcher
        self._send_xreq("")

    def _recv(self,timeout=None):
        """Internal method for receving a message."""
        socks = [self.disp_sock,self.ping_sock]
        ready = self._poll(socks,timeout=timeout)
        try:
            #  If we were pinged, respond to say we're still alive
            #  (or that we're shutting down)
            if self.ping_sock in ready:
                self.ping_sock.recv(zmq.NOBLOCK)
                if self._shutting_down:
                    self._send_xreq("X")
                else:
                    self._send_xreq("")
            #  Try to grab a request non-blockingly.
            return self._recv_xreq(zmq.NOBLOCK)
        except zmq.ZMQError, e:
            #  That didn't work out, return None.
            if e.errno != zmq.EAGAIN:
                if not self._has_shutdown:
                    raise
                if e.errno not in (zmq.ENOTSUP,zmq.EFAULT,):
                    raise
            return None

    def _send(self,server_id,client_ids,data):
        """Internal method to send out response data."""
        msg = "%s %d:%s, %s" % (server_id,len(client_ids),client_ids,data)
        self._send_xreq(msg)

    def _recv_xreq(self,flags=0):
        """Receive a message from the XREQ socket.

        This method contains the logic for stripping XREQ message delimiters,
        leaving just the message to be returned.
        """
        delim = self.disp_sock.recv(flags)
        if delim != "":
            return delim
        msg = self.disp_sock.recv(flags)
        return msg

    def _send_xreq(self,msg,flags=0):
        """Send a message through the XREQ socket.

        This method contains the logic for adding XREQ message delimiters
        to the message being sent.
        """
        self.disp_sock.send("",flags | zmq.SNDMORE)
        self.disp_sock.send(msg,flags)

    def shutdown(self,timeout=None):
        """Shut down the connection.

        This indicates that no more requests should be received by the
        handler, but it is willing to process any that have already been
        transmitted.  Use it for graceful termination of handlers.

        After shutdown, you may only call recv() with timeout=0.

        For the dispatcher connection, we send a single-byte message "X"
        to indicate that we're shutting down.  We then have to read in all
        incoming messages until the dispatcher responds with an "X" to
        indicate that the shutdown was recognised.
        """
        self._shutting_down = True
        self._send_xreq("X")
        t_start = time.time()
        msg = self._recv(timeout=timeout)
        t_end = time.time()
        timeout -= (t_end - t_start)
        while msg != "X" and (timeout is None or timeout > 0):
            if msg:
                self.recv_buffer.append(msg)
            t_start = time.time()
            msg = self._recv(timeout=timeout)
            t_end = time.time()
            timeout -= (t_end - t_start)
        super(DispatcherConnection,self).shutdown(timeout)

    def close(self):
        """Close the connection."""
        self.disp_sock.close()
        self.ping_sock.close()
        super(DispatcherConnection,self).close()



class Handler(object):
    """Mongrel2 request handler class.

    Instances of Handler act as a Mongrel2 handler process, dispatching
    incoming requests to their 'process_request' method.  The base class
    implementation does nothing.  See WSGIHandler for something useful.

    Handler objects must be constructed by passing in a Connection object.
    For convenience, you may pass a send_spec string instead of a Connection
    and one will be created for you.

    To enter the object's request-handling loop, call its 'serve' method; this
    will block until the server exits.  To exit the handler loop, call the
    'stop' method (probably from another thread).

    If you want more control over the handler, e.g. to integrate it with some
    other control loop, you can call the 'serve_one_request' method to serve
    a single request.  Note that this will block if there is not a request
    available - polling the underlying connection is up to you.
    """

    ConnectionClass = Connection

    def __init__(self,connection):
        self.started = False
        self.serving = False
        if isinstance(connection,basestring):
            self.connection = self.ConnectionClass(connection)
        else:
            self.connection = connection

    def serve(self):
        """Serve requests delivered by Mongrel2, until told to stop.

        This method is the main request-handling loop for Handler.  It calls
        the 'serve_one_request' method in a tight loop until told to stop
        by an explicit call to the 'stop' method.
        """
        self.serving = True
        self.started = True
        exc_info,exc_value,exc_tb = None,None,None
        try:
            while self.serving:
                self.serve_one_request()
        except Exception:
            self.running = False
            exc_info,exc_value,exc_tb = sys.exc_info()
            raise
        finally:
            #  Shut down the connection, but don't hide the original error.
            if exc_info is None:
                self._shutdown()
            else:
                try:
                    self._shutdown()
                except Exception:
                    print >>sys.stderr, "------- shutdown error -------"
                    traceback.print_exc()
                    print >>sys.stderr, "------------------------------"
                raise exc_info,exc_value,exc_tb

    def _shutdown(self):
        #  Attempt a clean disconnect from the socket.
        self.connection.shutdown(timeout=5)
        #  We have to handle anything that's already in our recv queue,
        #  or the requests will get lost when we close the socket.
        req = self.connection.recv(timeout=0)
        while req is not None:
            self.handle_request(req)
        self.wait_for_completion()
        self.connection.close()

    def stop(self):
        """Stop the request-handling loop and close down the connection."""
        self.serving = False
        self.started = False
        self.connection.interrupt()
    
    def serve_one_request(self,timeout=None):
        """Receive and serve a single request from Mongrel2."""
        req = self.connection.recv(timeout=timeout)
        if req is not None:
            self.handle_request(req)
        return req

    def handle_request(self,req):
        """Handle the given Request object.

        This method dispatches the given request object for processing.
        The base implementation just calls the process_request() method;
        subclasses might spawn a new thread or similar.

        It's a good idea to return control to the calling code as quickly as
        or you'll get a nice backlog of outstanding requests.
        """
        self.process_request(req)

    def process_request(self,req):
        """Process the given Request object.

        This method is the guts of a Mongrel2 handler, where you implement
        all your request-handling logic.  The base implementation does nothing.
        """
        pass

    def wait_for_completion(self):
        """Wait for all in-progress requests to be completed.

        Since the handle_request() method may operate asynchronously
        (e.g. by spawning a new thread) there must be a way to determine
        when all in-progress requets have been compeleted.  WSGIHandler
        subclasses should override this method to provide such behaviour.

        Note that the default implementation does nothing, since requests
        and handled synchronously in this case.
        """
        pass



class WSGIResponder(object):
    """Class for managing the WSGI response to a request.

    Instances of WSGIResponer manage the stateful details of responding
    to a particular Request object.  They provide the start_response callable
    and perform the internal buffering of status info required by the WSGI
    spec.

    Each WSGIResponder must be created with a single argument, the Request
    object to which it is responding.  You may then call the following methods
    to construct the response:

        start_response:  the standard WSGI start_response callable
        write:           send response data; doubles as the WSGI write callback
        finish           finalize the response, e.g. close the connection
        
    """

    def __init__(self,request):
        self.request = request
        self.status = None
        self.headers = None
        self.has_started = False
        self.is_chunked = False
        self.should_close = False

    def start_response(self,status,headers,exc_info=None):
        """Set the status code and response headers.

        This method provides the standard WSGI start_response callable.
        It just stores the given info internally for later use.
        """
        try:
            if self.has_started:
                if exc_info is not None:
                    raise exc_info[0], exc_info[1], exc_info[2]
                raise RuntimeError("response has already started")
            self.status = status
            self.headers = headers
            return self.write
        finally:
            exc_info = None

    def write(self,data):
        """Write the given data out on the response stream.

        This method sends the given data straight out to the client, sending
        headers and any framing info as necessary.
        """
        if not self.has_started:
            self.write_headers()
            self.has_started = True
        if self.is_chunked:
            self._write(hex(len(data))[2:])
            self._write("\r\n")
            self._write(data)
            self._write("\r\n")
        else:
            self._write(data)

    def finish(self):
        """Finalise the response.

        This method finalises the sending of the response, which may include
        sending a terminating data chunk or closing the connection.
        """
        if not self.has_started:
            self.write_headers()
            self.has_started = True
        if self.is_chunked:
            self._write("0\r\n\r\n")
        if self.should_close:
            self.request.disconnect()
            
    def write_headers(self):
        """Write out the response headers from the stored data.

        This method transmits the response headers stored from a previous call
        to start_response.  It also interrogates the headers to determine 
        various output modes, e.g. whether to use chunked encoding or whether
        to close the connection when finished.
        """
        self._write("HTTP/1.1 %s \r\n" % (self.status,))
        has_content_length = False
        has_date = False
        for (k,v) in self.headers:
            self._write("%s: %s\r\n" % (k,v,))
            if k.lower() == "content-length":
                has_content_length = True
            elif k.lower() == "date":
                has_date = True
        if not has_date:
            self._write("Date: %s\r\n" % (rfc822_format_date(),))
        if not has_content_length:
            if self.request.headers["VERSION"] == "HTTP/1.1":
                if self.request.headers["METHOD"] != "HEAD":
                    self._write("Transfer-Encoding: chunked\r\n")
                    self.is_chunked = True
            else:
                self.should_close = True
        self._write("\r\n")

    def _write(self,data):
        """Utility method for writing raw data to the response stream."""
        #  Careful; sending an empty string back to mongrel2 will
        #  cause the connection to be aborted!
        if data:
            self.request.respond(data)



class StreamingUploadFile(InputStream):
    """File-like object for streaming reads from in-progress uploads."""

    def __init__(self,request,fileobj):
        self.request = request
        self.fileobj = fileobj
        cl = request.headers.get("content-length","")
        if not cl:
            raise ValueError("missing content-length for streaming upload")
        try:
            cl = int(cl)
        except ValueError:
            msg = "malformed content-length for streaming upload: %r"
            raise ValueError(msg % (cl,))
        self.content_length = cl
        super(StreamingUploadFile,self).__init__()

    def close(self):
        super(StreamingUploadFile,self).close()
        self.fileobj.close()

    def _read(self,sizehint=-1):
        if self.fileobj.tell() >= self.content_length:
            return None
        if sizehint == 0:
            data = ""
        else:
            if sizehint > 0:
                data = self.fileobj.read(sizehint)
                while not data:
                    self._wait_for_data()
                    data = self.fileobj.read(sizehint)
            else:
                data = self.fileobj.read()
                while not data:
                    self._wait_for_data()
                    data = self.fileobj.read()
        return data

    def _wait_for_data(self):
        """Wait for more data to be available from this upload."""
        raise NotImplementedError


class WSGIHandler(Handler):
    """Mongrel2 Handler translating to WSGI.

    Instances of WSGIHandler act as Mongrel2 handler process, forwarding all
    requests to a WSGI application to provides a simple Mongrel2 => WSGI
    gateway.

    WSGIHandler objects must be constructed by passing in the target WSGI
    application and a Connection object.  For convenience, you may pass a
    send_spec string instead of a Connection and one will be created for you.

    To enter the object's request-handling loop, call its 'serve' method; this
    will block until the server exits.  To exit the handler loop, call the
    'stop' method (probably from another thread).

    If you want more control over the handler, e.g. to integrate it with some
    other control loop, you can call the 'serve_one_request' to serve a 
    single request.  Note that this will block if there is not a request
    available - polling the underlying connection is up to you.
    """

    ResponderClass = WSGIResponder
    StreamingUploadClass = StreamingUploadFile

    COMMA_SEPARATED_HEADERS = [
        'accept', 'accept-charset', 'accept-encoding', 'accept-language',
        'accept-ranges', 'allow', 'cache-control', 'connection',
        'content-encoding', 'content-language', 'expect', 'if-match',
        'if-none-match', 'pragma', 'proxy-authenticate', 'te', 'trailer',
        'transfer-encoding', 'upgrade', 'vary', 'via', 'warning',
        'www-authenticate'
    ]

    def __init__(self,application,connection):
        self.application = application
        super(WSGIHandler,self).__init__(connection)

    def process_request(self,req):
        """Process the given Request object.

        This method is the guts of the Mongrel2 => WSGI gateway.  It translates
        the mongrel2 request into a WSGI environ, invokes the application and
        sends the resulting response back to Mongrel2.
        """
        #  Mongrel2 uses JSON requests internally.
        #  We don't want them in our WSGI.
        if req.headers.get("METHOD","") == "JSON":
            return
        #  OK, it's a legitimate full HTTP request.
        #  Route it through the WSGI app.
        environ = {}
        responder = self.ResponderClass(req)
        try:
            #  If there's an async upload in progress, we have two options.
            #  If they sent a Content-Length header then we can do a streaming
            #  read from the file as it is being uploaded.  If there's no
            #  Content-Length then we have to wait for it all to upload (as
            #  there's no guarantee that the same handler will get both the
            #  start and end events for any upload).
            if "x-mongrel2-upload-start" in req.headers:
                if req.headers.get("content-length",""):
                    #  We'll streaming read it on the -start event,
                    #  so ignore the -done event.
                    if "x-mongrel2-upload-done" in req.headers:
                        return
                else:
                    #  We have to wait for the -done event,
                    #  so ignore the -start event.
                    if "x-mongrel2-upload-done" not in req.headers:
                        return
            #  Grab the full WSGI environ.
            #  This might error out, e.g. if someone tries any funny business
            #  with the mongrel2 upload headers.
            environ = self.get_wsgi_environ(req,environ)
            #  Call the WSGI app.
            #  Write all non-empty chunks, then clean up.
            chunks = self.application(environ,responder.start_response)
            try:
                for chunk in chunks:
                    if chunk:
                        responder.write(chunk)
                responder.finish()
            finally:
                if hasattr(chunks,"close"):
                    chunks.close()
        except Exception:
            print >>sys.stderr, "------- request handling error -------"
            traceback.print_exc()
            sys.stderr.write(str(environ) + "\n\n")
            print >>sys.stderr, "------------------------------ -------"
            #  Send an error response if we can.
            #  Always close the connection on error.
            if not responder.has_started:
                responder.start_response("500 Server Error",[],sys.exc_info())
                responder.write("server error")
                responder.finish()
            req.disconnect()
        finally:
            #  Make sure that the upload file is cleaned up.
            #  Mongrel doesn't reap these files itself, because the handler
            #  might e.g. move them somewhere.  We just read from them.
            try:
                environ["wsgi.input"].close()
            except (KeyError, AttributeError):
                pass
            upload_file = req.headers.get("x-mongrel2-upload-start",None)
            if upload_file:
                upload_file2 = req.headers.get("x-mongrel2-upload-done",None)
                if upload_file == upload_file2:
                    try:
                        os.unlink(upload_file)
                    except EnvironmentError:
                        pass

    def get_wsgi_environ(self,req,environ=None):
        """Construct a WSGI environ dict for the given Request object."""
        if environ is None:
            environ = {}
        #  Include keys required by the spec
        environ["REQUEST_METHOD"] = req.headers["METHOD"]
        script_name = req.headers["PATTERN"].split("(",1)[0]
        while script_name.endswith("/"):
            script_name = script_name[:-1]
        environ["SCRIPT_NAME"] = unquote_path(script_name)
        path_info = req.headers["PATH"][len(script_name):]
        environ["PATH_INFO"] = unquote_path(path_info)
        if "QUERY" in req.headers:
            environ["QUERY_STRING"] = unquote(req.headers["QUERY"])
        environ["SERVER_PROTOCOL"] = req.headers["VERSION"]
        #  TODO: mongrel2 doesn't seem to send me this info.
        #  How can I obtain it?  Suck it out of the config?
        #  Let's just hope the client sends a Host header...
        environ["SERVER_NAME"] = "localhost"
        environ["SERVER_PORT"] = "80"
        #  Include standard wsgi keys
        environ['wsgi.input'] = self.get_input_file(req)
        # TODO: 100-continue support?
        environ['wsgi.errors'] = sys.stderr
        environ['wsgi.version'] = (1,0)
        environ['wsgi.multithread'] = True
        environ['wsgi.multiprocess'] = False
        environ['wsgi.url_scheme'] = "http"
        environ['wsgi.run_once'] = False
        #  Include the HTTP headers
        for (k,v) in req.headers.iteritems():
            #  The mongrel2 headers dict contains lots of things
            #  other than the HTTP headers.
            if not k.islower() or "." in k:
                continue
            #  The list-like headers are helpfully already lists.
            #  Sadly, we have to put them back into strings for WSGI.
            if isinstance(v,list):
                if k in self.COMMA_SEPARATED_HEADERS:
                    v = ", ".join(v)
                else:
                    v = v[-1]
            environ["HTTP_" + k.upper().replace("-","_")] = v
        #  Grab some special headers into expected names
        ct = environ.pop("HTTP_CONTENT_TYPE",None)
        if ct is not None:
            environ["CONTENT_TYPE"] = ct
        cl = environ.pop("HTTP_CONTENT_LENGTH",None)
        if cl is not None:
            environ["CONTENT_LENGTH"] = cl
        return environ

    def get_input_file(self,req):
        """Get a file-like object for use as environ['wsgi.input']

        For small requests this is a StringIO object.  For large requests
        where an async upload is performed, it is the actual tempfile into
        which the upload was dumped.

        If the request contains a content-length, then we can read the upload
        file while it is still comming in.  If not, we wait for it to
        compelte and then use the raw file object.
        """
        upload_file = req.headers.get("x-mongrel2-upload-start",None)
        if not upload_file:
            return StringIO(req.body)
        upload_file2 = req.headers.get("x-mongrel2-upload-done",None)
        if upload_file2 is None:
            return self.StreamingUploadClass(req,open(upload_file,"rb"))
        else:
            if upload_file != upload_file2:
                #  Highly suspicious behaviour; terminate immediately.
                raise RuntimeError("mismatched mongrel2-upload header")
            return open(upload_file,"rb")


def test_application(environ,start_response):
    start_response("200 OK",[("Content-Length","3")])
    yield "OK\n"


if __name__ == "__main__":
    s = WSGIHandler(test_application,"tcp://127.0.0.1:9999")
    s.serve()


