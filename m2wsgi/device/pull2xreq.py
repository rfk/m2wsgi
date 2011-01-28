"""

m2wsgi.pull2xreq:  turn a PULL send socket into a XREQ send socket
==================================================================


This is a helper device implementing an experimental alternate protocol for
handlers to get requests out of Mongrel2.  It translates Mongrel2's standard
PULL-based request sending socket into a XREQ-based socket where the handlers
can explicitly chat with the socket.


Basic Usage
-----------

Suppose you have Mongrel2 pushing requests out to tcp://127.0.0.1:9999.
Instead of connecting your handlers directory to this socket, run the
pull2xreq device like so::

    python -m m2wsgi.device.pull2xreq \
              tcp://127.0.0.1:9999 tcp://127.0.0.1:8888

Then you can launch your handlers against the device's output socket and have
them chat with it about their availability.  Make sure you specify the XREQ
type for the send socket::

    m2wsgi --send-type=XREQ dotted.app.name \
           tcp://127.0.0.1:9989  tcp://127.0.0.1:9998
                   


OK, but why?
------------

In the standard PULL-based protocol, each handler process connects with a PULL
socket and requests are sent round-robin to all connected handlers.  This is
great for throughput but offers no way for a handler to cleanly disconnect -
if it goes offline with pending requests queued to it, those requests can 
get dropped.

In the XREQ-based protocol offered by this helper, each socket instead connects
to Mongrel2 (via this helper device) with a XREQ socket.  When it's ready for
work, the handler sends an explicit message identifying itself and Mongrel2
starts pushing it requests.

To effect a clean disconnect, the handler can send a special disconnect message
(the single byte "X") and this device will flush any queued requests, then
respond with an empty request.  At this point the handler knows that no more
requests will be sent its way, and it can safely terminate.

In the future this device may grow more complex request-routing features,
e.g. measuring the throughput of each connected handler and adjusting its
load accordingly.  Maybe.  Some day.

"""

import sys
import optparse
from textwrap import dedent
from collections import deque

import zmq.core.poll

from m2wsgi.util import encode_netstrings


def pull2xreq(in_spec,out_spec,in_ident=None,out_ident=None):
    """Run the pull2xreq translator device."""
    #  The input socket is a PULL socket from which we read requests.
    CTX = zmq.Context()
    in_sock = CTX.socket(zmq.PULL)
    if in_ident is not None:
        in_sock.setsockopt(zmq.IDENTITY,in_ident)
    in_sock.connect(in_spec)
    #  The output socket is an XREP socket.
    #  We read messages from connected handlers, and send requets to them.
    out_sock = CTX.socket(zmq.XREP)
    if out_ident is not None:
        out_sock.setsockopt(zmq.IDENTITY,out_ident)
    out_sock.bind(out_spec)

    avail_handlers = deque()
    disconnect_handlers = set()
    all_socks = [in_sock,out_sock]
    while True:
        (r,w,e) = zmq.core.poll.select(all_socks,all_socks,all_socks)
        if e:
            raise RuntimeError("sockets have errored out")
        #  If there's a message from a handler, process it and adjust
        #  the list of available/disconnected handlers accordingly.
        if out_sock in r:
            handler = out_sock.recv()
            delim = out_sock.recv()
            assert delim == "", "non-empty msg delimiter: "+delim
            msg = out_sock.recv()
            if msg == "X":
                disconnect_handlers.add(handler)
                if handler in avail_handlers:
                    avail_handlers.remove(handler)
            else:
                if handler not in avail_handlers:
                    avail_handlers.append(handler)
                if handler in disconnect_handlers:
                    disconnect_handlers.remove(handler)
        #  All our actions involve writing to the out socket,
        #  so we can't do anything until its ready.
        if out_sock in w:
            #  Process disconnect requests first, as they should be
            #  infrequent compared to incoming requests.
            if disconnect_handlers:
                handler = disconnect_handlers.pop()
                out_sock.send(handler,zmq.SNDMORE)
                out_sock.send("",zmq.SNDMORE)
                out_sock.send("")
            #  Then send any incoming request to the next available
            #  handler in the queue.
            elif in_sock in r and avail_handlers:
                req = in_sock.recv()
                handler = avail_handlers.popleft()
                out_sock.send(handler,zmq.SNDMORE)
                out_sock.send("",zmq.SNDMORE)
                out_sock.send(req)
                avail_handlers.append(handler)



if __name__ == "__main__":
    op = optparse.OptionParser(usage=dedent("""
    usage:  m2wsgi.device.pull2xreq [options] in_spec out_spec
    """))
    op.add_option("","--in-ident",type="str",default=None,
                  help="the in-socket identity to use")
    op.add_option("","--out-ident",type="str",default=None,
                  help="the out-socket identity to use")
    (opts,args) = op.parse_args()
    if len(args) != 2:
        raise ValueError("pull2xreq expects exactly two arguments")
    pull2xreq(*args,in_ident=opts.in_ident,out_ident=opts.out_ident)

