"""

m2wsgi.device.pull2xreq:  turn a PULL send socket into a XREQ send socket
========================================================================


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


Any Downsides?
--------------

Yes.  The protocol has no way of detecting handlers that die unexpectedly,
so it will keep sending requests to them which will be queued within zmq
until the handler reappears.

To mitigate this the device periodically resets its list of available
handlers, forcing them to re-contact the device for more work.  By default
this timer triggers every second (using SIGALRM).

Still working on a way to have the best of both worlds, but zmq doesn't seem
to deliver any disconnection info to userspace...

"""

import sys
import optparse
import signal
import errno
from textwrap import dedent
from collections import deque

import zmq.core.poll


class CheckableQueue(object):
    """Combine a deque and a set for fast queueing and membership checking."""
    def __init__(self):
        self.__members = set()
        self.__queue = deque()
    def __contains__(self,item):
        return item in self.__members
    def __len__(self):
        return len(self.__members)
    def clear(self):
        self.__members.clear()
        self.__queue.clear()
    def append(self,item):
        if item not in self.__members:
            self.__members.add(item)
            self.__queue.append(item)
    def popleft(self):
        while True:
            item = self.__queue.popleft()
            try:
                self.__members.remove(item)
            except ValueError:
                pass
            else:
                return item
    def remove(self,item):
        self.__members.remove(item)


class PULL2XREQ(object):
    """Device for translating a PULL-type send socket into a XREQ-type socket.

    """

    def __init__(self,in_spec,out_spec,in_ident=None,out_ident=None,
                 heartbeat_interval=1):
        self.in_spec = in_spec
        self.out_spec = out_spec
        self.in_ident = in_ident
        self.out_ident = out_ident or in_ident
        self.heartbeat_interval = heartbeat_interval
        self.ZMQ_CTX = zmq.Context()
        #  The input socket is a PULL socket from which we read requests.
        self.in_sock = self.ZMQ_CTX.socket(zmq.PULL)
        if self.in_ident is not None:
            self.in_sock.setsockopt(zmq.IDENTITY,self.in_ident)
        self.in_sock.connect(self.in_spec)
        #  The output socket is an XREP socket.
        #  We read messages from connected handlers, and send requets to them.
        self.out_sock = self.ZMQ_CTX.socket(zmq.XREP)
        if self.out_ident is not None:
            self.out_sock.setsockopt(zmq.IDENTITY,self.out_ident)
        self.out_sock.bind(self.out_spec)
        self.old_handlers = CheckableQueue()
        self.new_handlers = CheckableQueue()
        self.disconnecting_handlers = CheckableQueue()
        self.ready_requests = deque()
        self.alarm_state = 0

    def run(self):
        """Run the socket handlering loop."""
        signal.signal(signal.SIGALRM,self._alarm)
        while True:
            (r,w) = self.poll()
            if self.out_sock in r:
                self._read_handler()
            if self.in_sock in r:
                self._read_request()
            if self.out_sock in w:
                if self.disconnecting_handlers:
                    self._send_disconnect()
                elif self.ready_requests:
                    self._send_request()
            if self.alarm_state == 2:
                self.old_handlers.clear()
                self.alarm_state = 0

    def poll(self):
        """Get the sockets that are ready for work."""
        #  Get something to do, recovering from interrupted system calls.
        #  Only poll for writability if we have something to write.
        #  Otherwise we can busy-loop on the write-ready socket.
        socks = [self.in_sock,self.out_sock]
        try:
            wsocks = []
            if self.ready_requests:
                if self.new_handlers:
                    wsocks = socks
                elif self.old_handlers:
                    wsocks = socks
            elif self.disconnecting_handlers:
                wsocks = socks
            (r,w,e) = zmq.core.poll.select(socks,wsocks,socks)
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,):
                raise
            (r,w,e) = ([],[],[])
        if e:
            raise RuntimeError("sockets have errored out")
        print r,w
        return (r,w)

    def _alarm(self,sig,frm):
        print "ALARM"
        self.alarm_state = 2
        
    def _read_handler(self):
        handler = self.out_sock.recv()
        delim = self.out_sock.recv()
        assert delim == "", "non-empty msg delimiter: "+delim
        msg = self.out_sock.recv()
        #  If is a diconnect message, handlers goes into disconnecting_handlers
        if msg == "X":
            self.disconnecting_handlers.append(handler)
            try:
                self.new_handlers.remove(handler)
            except ValueError:
                pass
            try:
                self.old_handlers.remove(handler)
            except ValueError:
                pass
        #  If is a heartbeat message, handlers goes into new_handlers
        else:
            self.new_handlers.append(handler)
            try:
                self.disconnecting_handlers.remove(handler)
            except ValueError:
                pass
            try:
                self.old_handlers.remove(handler)
            except ValueError:
                pass

    def _read_request(self):
        req = self.in_sock.recv()
        self.ready_requests.append(req)
        if self.alarm_state == 0:
            signal.alarm(self.heartbeat_interval)
            self.alarm_state = 1

    def _send_disconnect(self):
        handler = self.disconnecting_handlers.popleft()
        self.out_sock.send(handler,zmq.SNDMORE)
        self.out_sock.send("",zmq.SNDMORE)
        self.out_sock.send("")

    def _send_request(self):
        handler = None
        try:
            handler = self.new_handlers.popleft()
        except IndexError:
            try:
                handler = self.old_handlers.popleft()
            except IndexError:
                pass
        if handler is not None:
            req = self.ready_requests.popleft()
            self.out_sock.send(handler,zmq.SNDMORE)
            self.out_sock.send("",zmq.SNDMORE)
            self.out_sock.send(req)
            self.old_handlers.append(handler)
        if self.alarm_state == 0:
            signal.alarm(self.heartbeat_interval)
            self.alarm_state = 1


def pull2xreq(in_spec,out_spec,in_ident=None,out_ident=None,
              heartbeat_interval=1):
    """Run the pull2xreq translator device."""
    PULL2XREQ(in_spec,out_spec,in_ident,out_ident,heartbeat_interval).run()


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

