"""

m2wsgi.base:  base handler implementation for m2wsgi
====================================================

This module contains the basic classes for implementing the m2wsgi handler.
If you don't explicitly select a different IO module, you'll get the
classes from in here.

"""

import os
import sys
import json
import time
import traceback
import threading
from cStringIO import StringIO

import zmq


def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module."""
    pass


def pop_netstring(data):
    """Pop a netstring from the front of the given data.

    This function pops a netstring-formatted chunk of data from the front
    of the given string.  It returns a two-tuple giving the contents of the
    netstring and any remaining data.
    """
    (size,body) = data.split(':', 1)
    size = int(size)
    if body[size] != ",":
        raise ValueError("not a netstring: %r" % (data,))
    return (body[:size],body[size+1:])


class Request(object):
    """Mongrel2 request object.

    This is a simple container object for the data in a Mongrel2 request.
    It can be used to poke around in the headers or body of the request,
    and to send replies to the request.
    """

    def __init__(self,connection,sender,cid,path,headers,body):
        self.connection = connection
        self.sender = sender
        self.path = path
        self.cid = cid
        self.headers = headers
        self.body = body

    @classmethod
    def parse(cls,connection,msg):
        """Parse a Request out of a Mongrel2 message.

        This is an alternate constructor for the class, which parses the
        various data fields out of the raw Mongrel2 message.
        """
        (sender,cid,path,rest) = msg.split(' ', 3)
        (headers,rest) = pop_netstring(rest)
        (body,_) = pop_netstring(rest)
        headers = json.loads(headers)
        return cls(connection,sender,cid,path,headers,body)

    def respond(self,data):
        """Send some response data to the issuer of this request."""
        self.connection.send(self.sender,self.cid,data)

    def close_connection(self):
        """Terminate the connection associated with this request."""
        self.connection.send(self.sender,self.cid,"")
 

class Connection(object):
    """Mongrel2 connection object.

    Instances of Connection represent a Handler connection to the main
    mongrel2 server.  They are used to receive requests and send responses.

    When creating a connection, you must specify at least the 'send_spec'
    argument.  This gives the ZMQ socket address on which to receive requests
    for processing from mongrel2.

    If specified, the 'recv_spec' argument gives the ZMQ socket address to send
    response data back to mongrel2.  If not specified, it defaults to the same
    host a send_spec and one port lower (which seems to be the convention for
    mongrel2 config).

    If specified, the 'send_ident' argument is used as the socket identity
    corresponding to 'send_spec'.  It should be a UUID string.  If not
    specified, the socket is run without identity and thus in non-durable mode.

    If specified, the 'recv_ident' argument is used as the socket identity
    corresponding to 'recv_spec'.  It should be a UUID string.  If not
    specified, the socket is run without identity and thus in non-durable mode.
    If not given, the default is to re-use 'send_ident'.
    """

    ZMQ_CTX = zmq.Context()

    RequestClass = Request

    def __init__(self,send_spec,recv_spec=None,send_ident=None,recv_ident=None):
        self.send_spec = send_spec
        if recv_spec is None:
            try:
                send_host,send_port = send_spec.rsplit(":",1)
                send_port = int(send_port)
            except (ValueError,TypeError):
                msg = "cannot guess recv_spec from send_spec %r" % (send_spec,)
                raise ValueError(msg)
            recv_spec = send_host + ":" + str(send_port - 1)
        self.recv_spec = recv_spec
        self.send_ident = send_ident
        self.recv_ident = recv_ident or send_ident
        self.reqs = self.ZMQ_CTX.socket(zmq.PULL)
        if self.send_ident is not None:
            self.reqs.setsockopt(zmq.IDENTITY,self.send_ident)
        self.reqs.connect(self.send_spec)
        self.resps = self.ZMQ_CTX.socket(zmq.PUB)
        if self.recv_ident is not None:
            self.resps.setsockopt(zmq.IDENTITY,self.recv_ident)
        self.resps.connect(self.recv_spec)

    def recv(self):
        msg = self.reqs.recv()
        return self.RequestClass.parse(self,msg)

    def send(self,target,cid,data):
        cid = str(cid)
        msg = "%s %d:%s, %s" % (target,len(cid),cid,data)
        self.resps.send(msg)

    def close(self):
        self.reqs.close()
        self.resps.close()


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
        flush:           finalize the response, e.g. close the connection
        
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

    def flush(self):
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
            self.request.close_connection()
            
    def write_headers(self):
        """Write out the response headers from the stored data.

        This method transmits the response headers stored from a previous call
        to start_response.  It also interrogates the headers to determine 
        various output modes, e.g. whether to use chunked encoding or whether
        to close the connection when finished.
        """
        self._write("HTTP/1.1 ")
        self._write(self.status)
        self._write("\r\n")
        has_content_length = False
        for (k,v) in self.headers:
            self._write(k)
            self._write(": ")
            self._write(v)
            self._write("\r\n")
            if k.lower() == "content-length":
                has_content_length = True
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



class WSGIHandler(object):
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
    available - polling to underlying connection is up to you.
    """

    ConnectionClass = Connection
    ResponderClass = WSGIResponder

    def __init__(self,application,connection):
        self.started = False
        self.serving = False
        self.application = application
        if isinstance(connection,basestring):
            self.connection = self.ConnectionClass(connection)
        else:
            self.connection = connection

    def serve(self):
        """Serve requests delivered by Mongrel2, until told to stop.

        This method is the main request-handling loop for WSGIHandler.  It
        calls the 'serve_one_request' method in a tight loop until told to
        stop by an explicit call to the 'stop' method.
        """
        self.serving = True
        self.started = True
        try:
            while self.serving:
                self.serve_one_request()
        except Exception:
            self.running = False
            raise
        finally:
            self.connection.close()

    def stop(self):
        """Stop the request-handling loop and close down the connection."""
        self.serving = False
        self.started = False
        self.connection.close()
    
    def serve_one_request(self):
        """Receive and serve a single request from Mongrel2."""
        req = self.connection.recv()
        #  Mongrel2 uses JSON requests internally.
        #  We don't want them in our WSGI.
        if req.headers.get("METHOD","") == "JSON":
            return
        self.handle_request(req)
        
    def handle_request(self,req):
        """Handle the given Request object.

        This method is the guts of the Mongrel2 => WSGI gateway.  It translates
        the mongrel2 request into a WSGI environ, invokes the application and
        sends the resulting response back to Mongrel2.

        Subclasses way override this method to control how and when the request
        is handled, e.g. by spawning a new thread or adding it to a work queue.
        It's a good idea to return control to the calling code as quickly as
        or you'll get a nice backlog of outstanding requests.
        """
        #  If there's an async upload in progress, wait until
        #  it completes.  Eventually we'll read this on demand.
        if "x-mongrel2-upload-start" in req.headers:
            if "x-mongrel2-upload-done" not in req.headers:
                return
        #  OK, it's a legitimate full HTTP request.
        #  Route it through the WSGI app.
        environ = {}
        responder = self.ResponderClass(req)
        try:
            #  Grab the real environ.
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
                responder.flush()
            finally:
                if hasattr(chunks,"close"):
                    chunks.close()
        except Exception:
            traceback.print_exc()
            #  Send an error response if we can.
            #  Always close the connection on error.
            if not responder.has_started:
                responder.start_response("500 Server Error",[],sys.exc_info())
                responder.write("server error")
                responder.flush()
            req.close_connection()
        finally:
            #  Make sure that the upload file is cleaned up.
            #  Mongrel doesn't reap these files itself, because the handler
            #  might e.g. move them somewhere.  We just read from them.
            environ["wsgi.input"].close()
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
        environ["SCRIPT_NAME"] = req.headers["PATTERN"]
        environ["PATH_INFO"] = req.headers["PATH"]
        if "QUERY" in req.headers:
            environ["QUERY_STRING"] = req.headers["QUERY"]
        environ["SERVER_PROTOCOL"] = req.headers["VERSION"]
        #  TODO: mongrel2 doesn't seem to send me this info.
        #  How can I obtain it?  Suck it out of the config?
        environ["SERVER_NAME"] = "localhost"
        environ["SERVER_PORT"] = "80"
        #  Include standard wsgi keys
        environ['wsgi.input'] = self.get_input_file(req)
        # TODO: 100-continue support
        environ['wsgi.errors'] = sys.stderr
        environ['wsgi.version'] = (1,0)
        environ['wsgi.multithread'] = True
        environ['wsgi.multiprocess'] = False
        environ['wsgi.url_scheme'] = "http"
        #  Include the HTTP headers
        for (k,v) in req.headers.iteritems():
            #  The mongrel2 headers dict contains lots of things
            #  other than the HTTP headers.
            if not k.islower() or "." in k:
                continue
            #  The list-like headers are helpfully already lists.
            #  Sadly, we have to put them back into strings.
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

        Eventually, I plan to support reading from the async upload tempfile
        while it is still being written, so that we get "streaming" of uploads
        directly into the WSGI app.  Obviously, that's not trivial.
        """
        upload_file = req.headers.get("x-mongrel2-upload-start",None)
        if not upload_file:
            return StringIO(req.body)
        upload_file2 = req.headers.get("x-mongrel2-upload-done",None)
        if upload_file != upload_file2:
            #  Highly suspicious behaviour; terminate immediately.
            raise RuntimeError("mismatched mongrel2-upload header")
        return open(upload_file,"rb")


if __name__ == "__main__":
    def application(environ,start_response):
        start_response("200 OK",[("Content-Length","11")])
        yield "hello world"
    s = WSGIHandler(application,"tcp://127.0.0.1:9999")
    s.serve()


