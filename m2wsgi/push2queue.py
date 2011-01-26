"""

m2wsgi.push2queue:  turn a PUSH socket into a REQ queue socket
--------------------------------------------------------------


This is a helper program to translate Mongrel2's standard PUSH-based request
sending socket into a REQ-based socket.  It is an experimental protocol to
allow handler processes to cleanly disconnect themselves.

In the standard PUSH-based protocol, each handler process connects with a PULL
socket and requests are send round-robin to all connected handlers.  This is
great for throughput but offers no way for a handler to cleanly disconnect -
if it goes offline with pending requests queued to it, those requests will 
be dropped.

In the REQ-based protocol offered by this helper, each socket instead connects
with a REQ socket.  When it's ready for more work, it sends a request and the
helper replies with a sequence of pending requests.  This allows the handler
to cleanly shut itself down - it simple stops asking for new requests.
Conceptually, it's quite similar to a standard queue of pending requests as
you might find in e.g. the CherryPy webserver.

This was inspired by the analysis of disconnect behaviour and proposed solution
by Samuel Tardieu:

    http://www.rfc1149.net/blog/2010/12/08/responsible-workers-with-0mq/

I've added the logic to send back more than one pending request in a single
response, to help increase throughput when there are lots of requests coming
in regularly (at least, that's the theory - I need to run some performance 
to compare this to the PUSH-based protocol).

"""

import sys
import zmq
from collections import deque

from m2wsgi.util import encode_netstrings


def push2queue(in_spec,out_spec,in_ident=None,out_ident=None):
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
        worker = out_sock.recv(flags)
        #  throw away XREP framing chunks
        out_sock.recv(flags); out_sock.recv(flags)
        return worker
    def send_requests(worker,reqs,flags=0):
        out_sock.send(worker,zmq.SNDMORE | flags)
        out_sock.send("",zmq.SNDMORE | flags)
        out_sock.send(encode_netstrings(reqs),flags)

    while True:
        #  Wait for an incoming request, then batch it together
        #  with any others that have arrived at the same time.
        reqs.append(in_sock.recv())
        try:
            while True:
                reqs.append(in_sock.recv(zmq.NOBLOCK))
        except zmq.ZMQError, e:
            if e.errno != zmq.EAGAIN:
                raise
        #  Wait for a ready worker, then gather any others
        #  that are also ready.  We may have ready workers left
        #  over from a previous iteration, in which case we
        #  don't block at all.
        if not workers:
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
            for _ in xrange(numreqs):
                if not reqs:
                    break
                yield reqs.popleft()
        #  Note that this loop will always empty reqs before or equal to
        #  when it empties workers, due to calculation of reqs_per_worker.
        while reqs:
            send_requests(workers.popleft(),popreqs())
            worker_num += 1
        #  We have have leftover ready workers here.  That's OK, they'll be
        #  given work on the next iteration.


if __name__ == "__main__":
    push2queue("tcp://127.0.0.1:9999","tcp://127.0.0.1:9989","XXX","YYY")
