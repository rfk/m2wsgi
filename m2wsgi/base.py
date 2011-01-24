
import os
import sys
import uuid
import json
import time
from cStringIO import StringIO

import zmq


def parse_netstring(ns):
    len, rest = ns.split(':', 1)
    len = int(len)
    assert rest[len] == ',', "Netstring did not end in ','"
    return rest[:len], rest[len+1:]


class Request(object):

    def __init__(self,connection,sender,cid,path,headers,body):
        self.connection = connection
        self.sender = sender
        self.path = path
        self.cid = cid
        self.headers = headers
        self.body = body

    @classmethod
    def recv_from(cls,connection):
        msg = connection.recv()
        sender, cid, path, rest = msg.split(' ', 3)
        headers, rest = parse_netstring(rest)
        body, _ = parse_netstring(rest)
        headers = json.loads(headers)
        return cls(connection,sender,cid,path,headers,body)

    def respond(self,data):
        self.connection.send(self.sender,self.cid,data)

    def close_connection(self):
        self.connection.send(self.sender,self.cid,"")
 


class Responder(object):

    def __init__(self,request):
        self.request = request
        self.status = None
        self.headers = None
        self.has_started = False
        self.is_chunked = False
        self.should_close = False

    def start_response(self,status,headers,exc_info=None):
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
        if not self.has_started:
            self.write_headers()
            self.has_started = True
        if self.is_chunked:
            self._write(self.req,hex(len(data))[2:])
            self._write(self.req,"\r\n")
            self._write(self.req,data)
            self._write(self.req,"\r\n")
        else:
            self._write(data)

    def flush(self):
        if not self.has_started:
            self.write_headers()
            self.has_started = True
        if self.is_chunked:
            self._write("0\r\n\r\n")
        if self.should_close:
            self.req.close_connection()
            
    def write_headers(self):
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
            if self.req.headers["VERSION"] == "HTTP/1.1":
                if self.req.headers["METHOD"] != "HEAD":
                    self._write("Transfer-Encoding: chunked\r\n")
                    self.is_chunked = True
            else:
                self.should_close = True
        self._write("\r\n")

    def _write(self,data):
        #  Careful; sending an empty string back to mongrel2 will
        #  cause the connection to be aborted!
        if data:
            self.request.respond(data)



class Connection(object):

    ZMQ_CTX = zmq.Context()

    def __init__(self,send_spec,recv_spec=None,send_ident=None):
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
        if send_ident is None:
            send_ident = str(uuid.uuid4())
        self.send_ident = send_ident
        self.reqs = self.ZMQ_CTX.socket(zmq.PULL)
        self.reqs.connect(self.send_spec)
        self.resps = self.ZMQ_CTX.socket(zmq.PUB)
        self.resps.setsockopt(zmq.IDENTITY,self.send_ident)
        self.resps.connect(self.recv_spec)

    def recv(self):
        return self.reqs.recv()

    def send(self,target,cid,data):
        cid = str(cid)
        msg = "%s %d:%s, %s" % (target,len(cid),cid,data)
        self.resps.send(msg)

    def close(self):
        self.reqs.close()
        self.resps.close()



class M2WSGI(object):
    """Mongrel2 Handler translating to wsgi."""

    RequestClass = Request
    ResponderClass = Responder
    ConnectionClass = Connection

    def __init__(self,application,*conn_args,**conn_kwds):
        self.started = False
        self.running = False
        self.application = application
        self.connection = self.ConnectionClass(*conn_args,**conn_kwds)

    def run(self):
        self.running = True
        self.started = True
        try:
            while self.running:
                self.serve_one_request()
        except (KeyboardInterrupt,):
            self.running = False
        finally:
            self.connection.close()

    def stop(self):
        if not self.started:
            raise RuntimeError("M2WSGI instance not yet started, cannot stop")
        self.running = False

    def serve_one_request(self):
        req = self.RequestClass.recv_from(self.connection)
        self.handle_request(req)
        
    def handle_request(self,req):
        print req.headers["PATH"]
        #  Mongrel2 uses JSON request internally.
        #  We don't want them in our WSGI.
        if req.headers.get("METHOD","") == "JSON":
            return
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
            #  This might error out, e.g. faked mongrel-upload headers.
            environ = self.get_wsgi_environ(req,environ)
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
            import traceback
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
            #  Mongrel doesn't seem to reap these files itself.
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
        if environ is None:
            environ = {}
        #  Include keys required by the spec
        environ["REQUEST_METHOD"] = req.headers["METHOD"]
        environ["SCRIPT_NAME"] = req.headers["PATTERN"]
        environ["PATH_INFO"] = req.headers["PATH"]
        if "QUERY" in req.headers:
            environ["QUERY_STRING"] = req.headers["QUERY"]
        environ["SERVER_NAME"] = "localhost"
        environ["SERVER_PORT"] = "80"
        environ["SERVER_PROTOCOL"] = req.headers["VERSION"]
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
        upload_file = req.headers.get("x-mongrel2-upload-start",None)
        if not upload_file:
            return StringIO(req.body)
        upload_file2 = req.headers.get("x-mongrel2-upload-done",None)
        if upload_file != upload_file2:
            #  Highly suspicious behaviour; terminate immediately.
            raise RuntimeError("mismatched upload file")
        return open(upload_file,"rb")


if __name__ == "__main__":
    def application(environ,start_response):
        start_response("200 OK",[("Content-Length","11")])
        yield "hello world"
    s = M2WSGI(application,"tcp://127.0.0.1:9999")
    s.run()

