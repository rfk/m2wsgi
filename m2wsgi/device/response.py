"""

m2wsgi.device.response:  device for sending a canned response
=============================================================


This is a simple device for sending a canned response to all requests.
You might use it for e.g. automatically redirecting based on host or route
information.   Use it like so::

    python -m m2wsgi.device.response \
              --code=302 --status="Moved Permanently"\
              --header-Location="www.example.com\\1"
              tcp://127.0.0.1:9999

"""

import sys

from m2wsgi.base import Connection


def response(conn,code=200,status="OK",headers={},body=""):
    """Run the response device."""
    if isinstance(conn,basestring):
        conn = Connection(conn)
    status_line = "HTTP/1.1 %d %s\r\n" % (code,status,)
    while True:
        req = conn.recv()
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


if __name__ == "__main__":
    #  We're going our own option parsing so we can handle
    #  arbitrary --header-<HEADER> options.
    def help():
        sys.stderr.write(dedent("""
        usage:  m2wsgi.device.response [options] send_spec [recv_spec]

            --code=CODE     the status code to send
            --status=MSG    the status message to send
            --header-K=V    a key/value header pair to send
            --body=BODY     the response body to send

            --send-ident    the send socket identity to use
            --send-type     the send socket type to use
            --recv-ident    the recv socket identity to use
            --recv-type     the recv socket type to use

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
        elif arg.startswith("--send-ident="):
            conn_kwds["send_ident"] = arg.split("=",1)[1]
        elif arg.startswith("--send-type="):
            conn_kwds["send_type"] = arg.split("=",1)[1]
        elif arg.startswith("--recv-ident="):
            conn_kwds["recv_ident"] = arg.split("=",1)[1]
        elif arg.startswith("--recv-type="):
            conn_kwds["recv_type"] = arg.split("=",1)[1]
        else:
            raise ValueError("unknown argument: %r" % (arg,))
        i += 1
    args = sys.argv[i:]
    if len(args) < 1:
        raise ValueError("response expects at least one argument")
    if len(args) > 2:
        raise ValueError("response expects at most two argument")
    conn = Connection(*args,**conn_kwds)
    response(conn,**kwds)

