"""

m2wsgi:  a mongrel2 => wsgi gateway and helper tools
====================================================


This module provides a WSGI gateway handler for the Mongrel2 webserver,
allowing easy deployment of python apps on Mongrel2.  It provides full support
for chunked response encoding, streaming reads of large file uploads, and
pluggable backends for evented I/O a la eventlet.

You might also find its supporting classes useful for developing non-WSGI
handlers in python.


Command-line usage
------------------

The simplest way to use this package is as a command-line launcher::

    m2wsgi dotted.app.name tcp://127.0.0.1:9999

This will connect to Mongrel2 on the specified request port and start handling
requests by passing them through the specified WSGI app.  By default you will
get a single worker thread handling all requests, which is probably not what
you want; increase the number of threads like so::

    m2wsgi --num-threads=5 dotted.app.name tcp://127.0.0.1:9999

If threads aren't your thing, you can just start several instances of the
handler pointing at the same zmq socket and get the same effect.  Better yet,
you can use eventlet to shuffle the bits around like so::

    m2wsgi --io=eventlet dotted.app.name tcp://127.0.0.1:9999

You can also use --io=gevent if that's how you roll.  Contributions for
other async backends are most welcome.


Programmatic Usage
------------------

If you have more complicated needs, you can use m2wsgi from within your
application.  The main class is 'WSGIHandler' which provides a simple
server interface.  The equivalent of the above command-line usage is::

    from m2wsgi.io.standard import WSGIHandler
    handler = WSGIHandler(my_wsgi_app,"tcp://127.0.0.1:9999")
    handler.serve()

There are a lot of "sensible defaults" being filled in here.  It assumes
that the Mongrel2 recv socket is on the next port down from the send socket,
and that it's OK to connect the socket without a persistent identity.

For finer control over the connection between your handler and Mongrel2,
create your own Connection object.  Here we use 127.0.0.1:9999 as the send
socket with identity AA-BB-CC, and use 127.0.0.2:9992 as the recv socket::

    from m2wsgi.io.standard import WSGIHandler, Connection
    conn = Connection(send_sock="tcp://AA-BB-CC@127.0.0.1:9999",
                      recv_sock="tcp://127.0.0.1:9992")
    handler = WSGIHandler(my_wsgi_app,conn)
    handler.serve()

If you're creating non-WSGI handlers for Mongrel2 you might find the following
classes useful:

    :Connection:  represents the connection from your handler to Mongrel2,
                  through which you can read requests and send responses.

    :Client:      represents a client connected to the server, to whom you
                  can send data at any time.

    :Request:     represents a client request to which you can asynchronously
                  send response data at any time.

    :Handler:     a base class for implementing handlers, with nothing
                  WSGI-specific in it.


Middleware
----------

If you need to add fancy features to the server, you can specify additional
WSGI middleware that should be applied around the application.  For example,
m2wsgi provides a gzip-encoding middleware that can be used to compress
response data::

    m2wsgi --middleware=GZipMiddleware
           dotted.app.name tcp://127.0.0.1:9999

If you want additional compression at the expense of WSGI compliance, you
can also do some in-server buffering before the gzipping is applied::

    m2wsgi --middleware=GZipMiddleware
           --middleware=BufferMiddleware
           dotted.app.name tcp://127.0.0.1:9999

The default module for loading middleware is m2wsgi.middleware; specify a
full dotted name to load a middleware class from another module.


Devices
-------

This module also provides a number of pre-built "devices" - stand-alone
executables designed to perform a specific common task.  Currently availble
devices are:

    :dispatcher:  implements a more flexible request-routing scheme than
                  the standard mongrel2 PUSH socket.

    :response:    implements a simple canned response, with ability to
                  interpolate variables from the request.



Don't we already have one of these?
-----------------------------------

Yes, there are several existing WSGI gateways for Mongrel2:

    * https://github.com/berry/Mongrel2-WSGI-Handler
    * https://bitbucket.org/dholth/mongrel2_wsgi

None of them fully met my needs.  In particular, this package has transparent
support for:

    * chunked response encoding
    * streaming reads of large "async upload" requests
    * pluggable IO backends (e.g. eventlet, gevent)

It's also designed from the ground up specifically for Mongrel2.  This means
it gets a lot of functionality for free, and the code is simpler and lighter
as a result.

For example, there is no explicit management of a threadpool and request queue
as you might find in e.g. the CherryPy server.  Instead, you just start up
as many threads as you need, have them all connect to the same handler socket,
and mongrel2 (via zmq) will automatically load-balance the requests to them.

Similarly, there's no fancy arrangement of master/worker processes to support
clean reloading of the handler; you just kill the old handler process and start
up a new one.  Send m2wsgi a SIGHUP and it will automatically shutdown and
reincarnate itself for a clean restart.


Current bugs, limitations and things to do
------------------------------------------

It's not all perfect just yet, although it does seem to mostly work:

    * Needs tests something fierce!  I just have to find the patience to
      write the necessary setup and teardown cruft.

    * It would be great to grab connection details straight from the
      mongrel2 config database.  Perhaps a Connection.from_config method
      with keywords to select the connection by handler id, host, route etc.

    * support for expect-100-continue; this may have to live in mongrel2

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.

__ver_major__ = 0
__ver_minor__ = 5
__ver_patch__ = 1
__ver_sub__ = ""
__version__ = "%d.%d.%d%s" % (__ver_major__,__ver_minor__,__ver_patch__,__ver_sub__)


import sys
import os
import optparse
from subprocess import MAXFD
from textwrap import dedent
try:
    import signal
except ImportError:
    signal = None


from m2wsgi.util import load_dotted_name


def main(argv=None):
    """Entry-point for command-line use of m2wsgi."""
    op = optparse.OptionParser(usage=dedent("""
    usage:  m2wsgi [options] dotted.app.name spend_spec [recv_spec]
    """))
    op.add_option("","--io",default="standard",
                  help="the I/O module to use")
    op.add_option("","--num-threads",type="int",default=1,
                  help="the number of threads to use")
    op.add_option("","--conn-type",default="",
                  help="the type of connection to use")
    op.add_option("","--middleware",action="append",
                  help="any middleware to apply to the wsgi app")
    (opts,args) = op.parse_args(argv)
    #  Sanity-check the arguments.
    if len(args) < 1:
        raise ValueError("you must specify the WSGI application")
    if opts.num_threads <= 0:
        raise ValueError("--num-threads must be positive")
    #  Grab the connection and handler class from the IO module
    conn_args = args[1:]
    iomod = "%s.io.%s" % (__name__,opts.io,)
    iomod = __import__(iomod,fromlist=["WSGIHandler"])
    iomod.monkey_patch()
    WSGIHandler = iomod.WSGIHandler
    if not opts.conn_type:
        Connection = iomod.Connection
    else:
        for nm in dir(iomod):
            if nm.lower() == opts.conn_type.lower() + "connection":
                Connection = getattr(iomod,nm)
                break
        else:
            raise ValueError("unknown connection type: %s" % (opts.conn_type,))
    #  Now that things are monkey-patched, we can load the app
    #  and the threading module.
    import threading
    app = load_dotted_name(args[0])
    assert callable(app), "the specified app is not callable"
    #  Apply any middleware that was specified in the args
    if opts.middleware:
       for mname in reversed(opts.middleware):
           if "." not in mname:
               mname = "m2wsgi.middleware." + mname
           mcls = load_dotted_name(mname)
           app = mcls(app)
    #  Try to clean up properly when killed.
    #  We turn SIGTERM into a KeyboardInterrupt exception.
    #  We catch SIGHUP and re-execute ourself as a simple reload mechanism.
    reload_the_process = []
    if signal is not None:
        def on_sigterm(*args):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM,on_sigterm)
        def on_sighup(*args):
            reload_the_process.append(True)
            raise KeyboardInterrupt
        signal.signal(signal.SIGHUP,on_sighup)
    #  Start the requested N handler threads.
    #  N-1 are started in background threads, then one on this thread.
    handlers = []
    threads = []
    def run_handler():
        conn = Connection(*conn_args)
        handler = WSGIHandler(app,conn)
        handlers.append(handler)
        handler.serve()
    for i in xrange(opts.num_threads - 1):
        t = threading.Thread(target=run_handler)
        t.start()
        threads.append(t)
    try:
        run_handler()
    except KeyboardInterrupt:
        if not reload_the_process:
            raise
    finally:
        #  When the handler exits, shut down any background threads.
        for h in handlers:
            h.stop()
        for t in threads:
            t.join()
    #  If we're doing a clean restart, close all fds and exec ourself.
    if reload_the_process:
        Connection.ZMQ_CTX.term()
        os.closerange(3,MAXFD)
        os.execv(sys.argv[0],sys.argv)


