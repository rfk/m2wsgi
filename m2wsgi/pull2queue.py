"""

m2wsgi.pull2queue:  turn a PULL socket into a REQ queue socket
==============================================================


This is a helper program implementing an experimental alternate protocol for
handlers to get requests out of Mongrel2.  It translates Mongrel2's standard
PULL-based request sending socket into a REQ-based socket where the handlers
are more in control.


Basic Usage
-----------

Suppose you have Mongrel2 pushing requests out to tcp://127.0.0.1:9999.
Instead of conneting your handlers directory to this socket, run the
pull2queue helper like so::

    python -m m2wsgi.pull2queue tcp://127.0.0.1:9999 tcp://127.0.0.1:9989

Then you can launch your handlers against the helper socket and have them
request work instead of it being pushed to them.  Make sure you specify the
REQ type for the send socket::

    python -m m2wsgi --send-type=REQ \
                     dotted.app.name \
                     tcp://127.0.0.1:9989 \
                     tcp://127.0.0.1:9998


OK, but why?
------------

In the standard PULL-based protocol, each handler process connects with a PULL
socket and requests are sent round-robin to all connected handlers.  This is
great for throughput but offers no way for a handler to cleanly disconnect -
if it goes offline with pending requests queued to it, those requests will 
be dropped.

In the REQ-based protocol offered by this helper, each socket instead connects
to Mongrel2 with a REQ socket.  When it's ready for more work, the handler
sends a request and Mongrel2 (via this helper script) replies with a sequence
of pending requests for it to process.  This allows the handler to cleanly 
shut itself down, by telling the socket not to send it any more requests.

Another benefit of this approach is that quick requests do not get assigned
to a handler that is busy with a slow request, which should help overall
throughput if each handler has limited internal concurrency.

Conceptually, the protocol is quite similar to a standard queue of pending
requests as you might find in e.g. the CherryPy webserver.  It was inspired
by this post on disconnect behaviour by Samuel Tardieu:

    http://www.rfc1149.net/blog/2010/12/08/responsible-workers-with-0mq/

I've added the logic to send back more than one pending request in a single
response, to help increase throughput when there are lots of requests coming
in regularly (at least, that's the theory - I need to run some performance 
tests to compare this to the PULL-based protocol).

"""

import sys
import optparse
from textwrap import dedent
from collections import deque

import zmq

from m2wsgi.util import encode_netstrings


def pull2queue(in_spec,out_spec,in_ident=None,out_ident=None,max_batch_size=10):
    """Run the pull2queue helper program."""
    CTX = zmq.Context()
    in_sock = CTX.socket(zmq.PULL)
    if in_ident is not None:
        in_sock.setsockopt(zmq.IDENTITY,in_ident)
    in_sock.connect(in_spec)
    out_sock = CTX.socket(zmq.XREP)
    if out_ident is not None:
        out_sock.setsockopt(zmq.IDENTITY,out_ident)
    out_sock.bind(out_spec)

    reqs = deque()
    workers = deque()
    def recv_worker(flags=0):
        while True:
            worker = out_sock.recv(flags)
            assert out_sock.recv(flags) == ""
            msg = out_sock.recv(flags)
            if msg == "X":
                #  it's a disconnect request
                try:
                    workers.remove(worker)
                except ValueError:
                    pass
                disconnect_worker(worker)
            else:
                #  that worker is good to go
                return worker
    def send_requests(worker,reqs,flags=0):
        reqs = encode_netstrings(reqs)
        out_sock.send(worker,zmq.SNDMORE | flags)
        out_sock.send("",zmq.SNDMORE | flags)
        out_sock.send(reqs,flags)
    def disconnect_worker(worker):
        out_sock.send(worker,zmq.SNDMORE)
        out_sock.send("",zmq.SNDMORE)
        out_sock.send("")

    while True:
        #  Wait for an incoming request, then batch it together
        #  with any others that have arrived at the same time.
        if not reqs:
            reqs.append(in_sock.recv())
        try:
            while True:
                reqs.append(in_sock.recv(zmq.NOBLOCK))
        except zmq.ZMQError, e:
            if e.errno != zmq.EAGAIN:
                raise
        #  Wait for a ready worker, then gather any others
        #  that are also ready for work.
        #  Note that we must loop here, because workers can be removed
        #  from the ready list if they explicitly disconnect.
        while not workers:
            workers.append(recv_worker())
            try:
                while True:
                    workers.append(recv_worker(zmq.NOBLOCK))
            except zmq.ZMQError, e:
                if e.errno != zmq.EAGAIN:
                    raise
        #  Split the pending requests evenly amongst the workers.
        (reqs_per_worker,remainder) = divmod(len(reqs),len(workers))
        worker_num = 0
        def popreqs():
            numreqs = reqs_per_worker
            if remainder > worker_num:
                numreqs += 1
            if numreqs > max_batch_size:
                numreqs = max_batch_size
            for _ in xrange(numreqs):
                if not reqs:
                    break
                yield reqs.popleft()
        while reqs and workers:
            send_requests(workers.popleft(),popreqs())
            worker_num += 1
        #  We may have have leftover ready workers and/or ready requests here.
        #  That's OK, they'll be dealth with on the next iteration.


if __name__ == "__main__":
    op = optparse.OptionParser(usage=dedent("""
    usage:  m2wsgi.pull2queue [options] in_spec out_spec
    """))
    op.add_option("","--in-ident",type="str",default=None,
                  help="the in-socket identity to use")
    op.add_option("","--out-ident",type="str",default=None,
                  help="the out-socket identity to use")
    op.add_option("","--max-batch-size",type="int",default=10,
                  help="max requests to send out in single batch")
    (opts,args) = op.parse_args()
    if len(args) != 2:
        raise ValueError("pul2queue expects exactly two arguments")
    pull2queue(*args,in_ident=opts.in_ident,out_ident=opts.out_ident,
                     max_batch_size=opts.max_batch_size)

