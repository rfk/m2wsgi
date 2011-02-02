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
Instead of connecting your handlers directly to this socket, run the
pull2xreq device like so::

    python -m m2wsgi.device.pull2xreq \
              tcp://127.0.0.1:9999 tcp://127.0.0.1:8888

Then you can launch your handlers against the device's output socket and have
them chat with it about their availability.  Make sure you specify the XREQ
type for the send socket::

    m2wsgi --send-type=XREQ dotted.app.name \
           tcp://127.0.0.1:8888  tcp://127.0.0.1:9998



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
    """Combine a deque and a set for O(1) queueing and membership checking."""
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
            except KeyError:
                pass
            else:
                return item
    def remove(self,item):
        try:
            self.__members.remove(item)
        except KeyError:
            raise ValueError


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
        #  We read messages from connected handlers, and send requests to them.
        self.out_sock = self.ZMQ_CTX.socket(zmq.XREP)
        if self.out_ident is not None:
            self.out_sock.setsockopt(zmq.IDENTITY,self.out_ident)
        self.out_sock.bind(self.out_spec)
        self.ready_requests = deque()
        self.active_handlers = CheckableQueue()
        self.suspended_handlers = CheckableQueue()
        self.disconnecting_handlers = CheckableQueue()
        self.alarm_state = 0

    def _whatis(self,s):
        if s is self.in_sock:
            return "in"
        if s is self.out_sock:
            return "out"
        return s

    def run(self):
        """Run the socket handlering loop."""
        signal.signal(signal.SIGALRM,self._alarm)
        while True:
            (r,w) = self.poll()
            #  When the alarm goes, suspend all handlers.
            #  They have to prove they're still alive to get more work.
            if self.alarm_state == 2:
                self._suspend_active_handlers()
                self.alarm_state = 0
            #  If we have active handlers, make sure an alarm is set.
            #  This lets us keep the heartbeat going without waking up
            #  all the time when there's nothing to do.
            elif self.alarm_state == 0:
                if self.active_handlers:
                    signal.alarm(self.heartbeat_interval)
                    self.alarm_state = 1
            print map(self._whatis,r), map(self._whatis,w)
            print len(self.active_handlers), len(self.suspended_handlers)
            #  If we have a request ready, try to wake up the
            #  suspended handlers so we can serve it.
            if self.suspended_handlers:
                if self.in_sock in r or self.ready_requests:
                    self._wake_suspended_handlers()
            #  Disconnect any handlers that are waiting for it.
            #  This will include anything we want to wake up.
            if self.disconnecting_handlers:
                self._send_disconnect_messages()
            #  Handle any messages from handlers.  They might be
            #  coming back alive, or wanting to disconnect.
            if self.out_sock in r:
                self._read_handler_messages()
            #  If we have some active handlers, we can dispatch a request.
            #  Note that we only send a single request then re-enter
            #  this loop, to give handlers a chance to wake up.
            if self.active_handlers:
                if self.in_sock in r or self.ready_requests:
                    self._dispatch_request()

    def poll(self):
        """Get the sockets that are ready for work."""
        rsocks = [self.out_sock]
        wsocks = []
        esocks = [self.in_sock,self.out_sock]
        try:
            #  Don't poll for new requests if we don't have handlers,
            #  or if we already have requests in memory.
            #  Don't check writability of out socket unless we need
            #  to actually write to it.
            if self.active_handlers or self.suspended_handlers:
                if not self.ready_requests:
                    rsocks.append(self.in_sock)
                else:
                    wsocks.append(self.out_sock)
            if self.disconnecting_handlers and not wsocks:
                wsocks.append(self.out_sock)
            (r,w,e) = zmq.core.poll.select(rsocks,wsocks,esocks)
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,):
                raise
            return ([],[])
        else:
            if e:
                raise RuntimeError("sockets have errored out")
            return (r,w)

    def _alarm(self,sig,frm):
        """Handler for SIGALRM.

        This simply sets self.alarm_state to note that the alarm has
        gone off.  The signal will also interrupt self.poll() and then
        we can deal with the alarm in the main run loop.
        """
        print "ALARM"
        self.alarm_state = 2

    def _suspend_active_handlers(self):
        """Move all active handlers to the suspended state."""
        while self.active_handlers:
            handler = self.active_handlers.popleft()
            self.suspended_handlers.append(handler)

    def _wake_suspended_handlers(self):
        """Force all suspended handlers to wake up if they're still alive.

        Actually this just moves them into the "disconnecting" state.
        """
        while self.suspended_handlers:
            handler = self.suspended_handlers.popleft()
            self.disconnecting_handlers.append(handler)

    def _send_disconnect_messages(self):
        """Send disconnection messages to anyone who needs it.

        This will give the handler a chance to either report back that
        it's still alive, or terminate cleanly.
        """
        try:
            while True:
                handler = self.disconnecting_handlers.popleft()
                self.out_sock.send(handler,zmq.SNDMORE|zmq.NOBLOCK)
                self.out_sock.send("",zmq.SNDMORE|zmq.NOBLOCK)
                self.out_sock.send("",zmq.NOBLOCK)
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise
        except IndexError:
            pass
        
    def _read_handler_messages(self):
        """Read messages coming in from handlers.

        This might be heartbeat messages letting us know the handler is
        still alive, or explicit disconnect messages.
        """
        try:
            handler = self.out_sock.recv(zmq.NOBLOCK)
            delim = self.out_sock.recv(zmq.NOBLOCK)
            assert delim == "", "non-empty msg delimiter: "+delim
            msg = self.out_sock.recv(zmq.NOBLOCK)
            #  A diconnect message
            if msg == "X":
                self._mark_handler_disconnecting(handler)
            #  A heartbeat message
            else:
                self._mark_handler_active(handler)
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise

    def _mark_handler_disconnecting(self,handler):
        """Mark the given handler as disconncting.

        We'll try to send it a disconnect message as soon as possible.
        """
        self.disconnecting_handlers.append(handler)
        try:
           self.active_handlers.remove(handler)
        except ValueError:
            pass
        try:
            self.suspended_handlers.remove(handler)
        except ValueError:
            pass

    def _mark_handler_active(self,handler):
        """Mark the given handler as active.

        This means we can dispatch requests to this handler and have a
        reasonable chance of them being handled.
        """
        self.active_handlers.append(handler)
        try:
           self.disconnecting_handlers.remove(handler)
        except ValueError:
            pass
        try:
            self.suspended_handlers.remove(handler)
        except ValueError:
            pass

    def _dispatch_request(self):
        """Dispatch a single request to an active handler."""
        #  Grab a request, either from memory or from the socket.
        if self.ready_requests:
            req = self.ready_requests.popleft()
        else:
            try:
                req = self.in_sock.recv(zmq.NOBLOCK)
            except zmq.ZMQError, e:
                if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                    raise
                return
        #  Find an active handler to dispatch it to.
        #  If this fails for whatever reason, keep thje request
        #  queued in memory to try again later.
        try:
            while req is not None:
                handler = self.active_handlers.popleft()
                try:
                    self.out_sock.send(handler,zmq.SNDMORE|zmq.NOBLOCK)
                    self.out_sock.send("",zmq.SNDMORE|zmq.NOBLOCK)
                    self.out_sock.send(req,zmq.NOBLOCK)
                except zmq.ZMQError, e:
                    if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                        raise
                else:
                    req = None
                finally:
                    self.active_handlers.append(handler)
        except IndexError:
            pass
        finally:
            if req is not None:
                self.ready_requests.append(req)


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

