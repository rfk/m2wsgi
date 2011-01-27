

m2wsgi:  a mongrel2 => wsgi gateway
===================================


This module provides a WSGI gateway handler for the Mongrel2 webserver,
allowing easy deployment of python apps on Mongrel2.  You might also find
its supporting classes useful for developing non-WSGI handlers in python.


Command-line usage
------------------

The simplest way to use this package is as a command-line launcher::

    python -m m2wsgi dotted.app.name tcp://127.0.0.1:9999

This will connect to Mongrel2 on the specified request port and start handling
requests by passing them through the specified WSGI app.  By default you will
get a single worker thread handling all requests, which is probably not what
you want; increase the number of threads like so::

    python -m m2wsgi --num-threads=5 dotted.app.name tcp://127.0.0.1:9999

Or if threads aren't your thing, use eventlet to shuffle the bits around
like so::

    python -m m2wsgi --io=eventlet dotted.app.name tcp://127.0.0.1:9999

I'm interested in adding support for other IO modules such as gevent;
contributions welcome.


Programmatic Usage
------------------

If you have more complicated needs, you can use m2wsgi from within your
application.  The main class is 'WSGIHandler' which provides a simple
server interface.  The equivalent of the above command-line usage is::

    from m2wsgi.base import WSGIHandler
    handler = WSGIHandler(my_wsgi_app,"tcp://127.0.0.1:9999")
    handler.serve()

For finer control over the connection between your handler and Mongrel2,
create your own Connection object::

    from m2wsgi.base import WSGIHandler, Connection
    conn = Connection(send_spec="tcp://127.0.0.1:9999",
                      recv_spec="tcp://127.0.0.1:9999",
                      send_ident="9a5eee79-dbd5-4f33-8fd0-69b304c6035a")
    handler = WSGIHandler(my_wsgi_app,conn)
    handler.serve()


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

Similarly, there's no explicit support for reloading when the code changes.
Just kill the old handler and start up a new one.  If you're using fixed
handler UUIDs then zmq will ensure that the handover happens gracefully.


Current bugs, limitations and things to do
------------------------------------------

It's not all perfect just yet, although it does seem to mostly work:

    * Needs tests something fierce!  I just have to find the patiences to
      write all the setup and teardown cruft.

    * When running multiple threads, ctrl-C doesn't cleanly exit the process.
      Seems like the background threads get stuck in a blocking recv().
      I *really* don't want to emulate interrupts using zmq_poll...

    * The zmq load-balancing algorithm is greedy round-robin, which isn't
      ideal.  For example, it can schedule several fast requests to the same
      thread as a slow one, making them wait even if other threads become
      available.  I'm working on a zmq adapter that can do something better
      (see the pull2queue script in this distribution).

    * It would be great to grab connection details straight from the
      mongrel2 config database.  Perhaps a Connection.from_config method
      with keywords to select the connection by handler id, host, route etc.


