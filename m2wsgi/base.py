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
import thread
import time
import traceback
from collections import deque
from cStringIO import StringIO

import zmq

from m2wsgi.util import pop_netstring, unquote_path, unquote, InputStream


def monkey_patch():
    """Hook to monkey-patch the interpreter for this IO module."""
    pass



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
        (headers_str,rest) = pop_netstring(rest)
        (body,_) = pop_netstring(rest)
        headers = {}
        for (k,v) in json.loads(headers_str).iteritems():
            headers[k.encode("ascii")] = v.encode("ascii")
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
    Mongrel2 server.  They are used to receive requests and send responses.

    When creating a connection, you must specify at least the 'send_spec'
    argument.  This gives the ZMQ socket address on which to receive requests
    for processing from mongrel2.

    If specified, the 'recv_spec' argument gives the ZMQ socket address to send
    response data back to mongrel2.  If not specified, it defaults to the same
    host a send_spec and one port lower (which seems to be the convention for
    mongrel2 config).

    If specified, the 'send_ident' and 'recv_ident' arguments give the socket
    identities for the send and recv sockts.  They should be UUID strings.
    If only send_ident is specified, recv_ident will default to it.  If neither
    are specified, the sockets will run without identity in non-durable mode.

    If specified, the 'send_type' and 'recv_type' arguments give the type
    of socket to use.  The defaults are zmq.PULL and zmq.PUB respectively.
    Currently supported send socket types are:

        * zmq.PULL:  The standard push socket offered by Mongrel2.  This
                     is very efficient but offers no way to detect handlers
                     that have disconnected.

        * zmq.REQ:   A request-response socket.  The handler explicitly asks
                     for new requests.  This is a little slower but allows
                     clients to cleanly disconnect (they just stop asking for
                     requests).  For now, you must run the "push2queue" script
                     included in this module to translate mongrel2's PULL
                     socket into an XREP socket to connect in this mode.

    Currently supported recv socket types are:

        * zmq.PUB:  The standard publisher socket offered by Mongrel2.
                    Response data is published, prefixed with the identity
                    of the mongrel2 instance managing the request.

    """

    ZMQ_CTX = zmq.Context()

    RequestClass = Request

    def __init__(self,send_spec,recv_spec=None,send_ident=None,recv_ident=None,
                      send_type=None,recv_type=None):
        self.send_spec = send_spec
        if recv_spec is None:
            try:
                send_host,send_port = send_spec.rsplit(":",1)
                send_port = int(send_port)
            except (ValueError,TypeError):
                msg = "cannot guess recv_spec from send_spec %r" % (send_spec,)
                raise ValueError(msg)
            recv_spec = send_host + ":" + str(send_port - 1)
        if send_type is None:
            send_type = zmq.PULL
        elif isinstance(send_type,basestring):
            send_type = getattr(zmq,send_type)
        if recv_type is None:
            recv_type = zmq.PUB
        elif isinstance(recv_type,basestring):
            recv_type = getattr(zmq,recv_type)
        self.recv_spec = recv_spec
        self.send_ident = send_ident
        self.recv_ident = recv_ident or send_ident
        self.send_type = send_type
        self.recv_type = recv_type
        self.recv_buffer = deque()
        self.reqs = self.ZMQ_CTX.socket(self.send_type)
        if self.send_ident is not None:
            self.reqs.setsockopt(zmq.IDENTITY,self.send_ident)
        self.reqs.connect(self.send_spec)
        self.resps = self.ZMQ_CTX.socket(self.recv_type)
        if self.recv_ident is not None:
            self.resps.setsockopt(zmq.IDENTITY,self.recv_ident)
        self.resps.connect(self.recv_spec)

    def recv(self,blocking=True):
        """Receive a request from the send socket.

        This method receives a request from the send socket, parses it into
        a Request object and returns it.
 
        You may specify the keyword argument 'blocking' to request blocking
        (the default) or non-blocking recv.  If not blocking and no request
        is available, None is returned.
        """
        #  For a PULL socket, we just recv the message straight out of it.
        if self.send_type == zmq.PULL:
            if blocking:
                msg = self.reqs.recv()
            else:
                try:
                    msg = self.reqs.recv(zmq.NOBLOCK)
                except zmq.ZMQError, e:
                    if e.errno != zmq.EAGAIN:
                        raise
                    return None
        #  For a REQ socket, we send an empty request message to it,
        #  and get back a sequence of requests.  We return the first and
        #  put the remainder into the recv_buffer.
        elif self.send_type == zmq.REQ:
            if not self.recv_buffer:
                if not blocking:
                    return None
                self.reqs.send("")
                msgs = self.reqs.recv()
                while msgs:
                    (msg,msgs) = pop_netstring(msgs)
                    self.recv_buffer.append(msg)
            msg = self.recv_buffer.popleft()
        else:
            raise ValueError("unknown send_type: %s" % (self.send_type,))
        return self.RequestClass.parse(self,msg)

    def send(self,target,cid,data):
        """Send a response to the specified target mongrel2 instance."""
        cid = str(cid)
        msg = "%s %d:%s, %s" % (target,len(cid),cid,data)
        if self.recv_type == zmq.PUB:
            self.resps.send(msg)
        else:
            raise ValueError("unknown recv_type: %s" % (self.recv_type,))

    def close(self):
        """Close the connection."""
        self.close_send()
        self.close_recv()

    def close_send(self):
        """Close the send-side of the connection."""
        self.reqs.close()

    def close_recv(self):
        """Close the recv-side of the connection."""
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
        """Wait for more data to be available from this upload.

        The base implementation simply does a sleep loop until the
        file grows past its current position.  Eventually we could try
        using file notifications to detect change.
        """
        curpos = self.fileobj.tell()
        cursize = os.fstat(self.fileobj.fileno()).st_size
        while curpos >= cursize:
            time.sleep(0.01)
            cursize = os.fstat(self.fileobj.fileno()).st_size


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
    StreamingUploadClass = StreamingUploadFile

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
            e1,e2,e3 = sys.exc_info()
            try:
                self._shutdown()
            except Exception:
                traceback.print_exc()
            raise e1,e2,e3
        else:
            self._shutdown()

    def _shutdown(self):
        #  We have to handle anything that's already in our recv queue,
        #  or the requests will get lost when we close the socket.
        reqs = deque()
        req = self.connection.recv(blocking=False)
        while req is not None:
            reqs.append(req)
        self.connection.close_send()
        for req in reqs:
            self.handle_request(req)
        # TODO: handle_request might spawn a new thread, need to wait for
        # them to compelete.
        self.connection.close()

    def stop(self):
        """Stop the request-handling loop and close down the connection."""
        self.serving = False
        self.started = False
        # TODO: how to interrupt blocking recv?
    
    def serve_one_request(self):
        """Receive and serve a single request from Mongrel2."""
        req = self.connection.recv()
        #print req.headers["PATH"], thread.get_ident()
        self.handle_request(req)
        
    def handle_request(self,req):
        """Handle the given Request object.

        This method is the guts of the Mongrel2 => WSGI gateway.  It translates
        the mongrel2 request into a WSGI environ, invokes the application and
        sends the resulting response back to Mongrel2.

        Subclasses may override this method to control how and when the request
        is handled, e.g. by spawning a new thread or adding it to a work queue.
        It's a good idea to return control to the calling code as quickly as
        or you'll get a nice backlog of outstanding requests.
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
                responder.flush()
            finally:
                if hasattr(chunks,"close"):
                    chunks.close()
        except Exception:
            traceback.print_exc()
            sys.stderr.write(str(environ) + "\n\n")
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
            try:
                environ["wsgi.input"].close()
            except KeyError:
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
        script_name = req.headers.get("PREFIX",req.headers["PATTERN"])
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


if __name__ == "__main__":
    def application(environ,start_response):
        start_response("200 OK",[("Content-Length","11")])
        yield "hello world"
    s = WSGIHandler(application,"tcp://127.0.0.1:9999")
    s.serve()


