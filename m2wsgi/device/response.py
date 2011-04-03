"""

m2wsgi.device.response:  device for sending a canned response
=============================================================


This is a simple device for sending a canned response to all requests.
You might use it for e.g. automatically redirecting based on host or route
information.   Use it like so::

    #  Redirect requests to canonical domain
    python -m m2wsgi.device.response \
              --code=302
              --status="Moved Permanently"\
              --header-Location="http://www.example.com%(PATH)s"
              --body="Redirecting to http://www.example.com\r\n"
              tcp://127.0.0.1:9999


Some things to note:

    * you can separately specify the status code, message, headers and body.

    * the body and headers can contain python string-interpolation patterns,
      which will be filled in with values from the request headers.

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.


import sys
import traceback

from m2wsgi.io.standard import Connection


def response(conn,code=200,status="OK",headers={},body=""):
    """Run the response device."""
    if isinstance(conn,basestring):
        conn = Connection(conn)
    status_line = "HTTP/1.1 %d %s\r\n" % (code,status,)
    while True:
        req = conn.recv()
        try:
            prefix = req.headers.get("PATTERN","").split("(",1)[0]
            req.headers["PREFIX"] = prefix
            req.headers["MATCH"] = req.headers.get("PATH","")[len(prefix):]
            req.headers.setdefault("host","")
            req.respond(status_line)
            for (k,v) in headers.iteritems():
                req.respond(k)
                req.respond(": ")
                req.respond(v % req.headers)
                req.respond("\r\n")
            rbody = body % req.headers
            req.respond("Content-Length: %d\r\n\r\n" % (len(rbody),))
            if rbody:
                req.respond(rbody)
        except Exception:
            req.disconnect()
            traceback.print_exc()


if __name__ == "__main__":
    #  We're doing our own option parsing so we can handle
    #  arbitrary --header-<HEADER> options.  It's really not so hard...
    def help():
        sys.stderr.write(dedent("""
        usage:  m2wsgi.device.response [options] send_spec [recv_spec]

            --code=CODE     the status code to send
            --status=MSG    the status message to send
            --header-K=V    a key/value header pair to send
            --body=BODY     the response body to send

        """))
    kwds = {}
    conn_kwds = {}
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if not arg.startswith("-"):
            break
        if arg.startswith("--code="):
            kwds["code"] = int(arg.split("=",1)[1])
        elif arg.startswith("--status="):
            kwds["status"] = arg.split("=",1)[1]
        elif arg.startswith("--header-"):
            (k,v) = arg[len("--header-"):].split("=",1)
            kwds.setdefault("headers",{})[k] = v
        elif arg.startswith("--body="):
            kwds["body"] = arg.split("=",1)[1]
        else:
            raise ValueError("unknown argument: %r" % (arg,))
        i += 1
    args = sys.argv[i:]
    if len(args) < 1:
        raise ValueError("response expects at least one argument")
    if len(args) > 2:
        raise ValueError("response expects at most two argument")
    conn = Connection(*args)
    response(conn,**kwds)

