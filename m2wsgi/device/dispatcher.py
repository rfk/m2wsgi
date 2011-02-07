"""

m2wsgi.device.dispatcher:  general-purpose request dispatching hub
==================================================================


This is a device for receiving requests from mongrel2 and dispatching them
to handlers.  It's designed to give you more flexibility over routing than
using a raw PUSH socket.


Basic Usage
-----------

Suppose you have Mongrel2 pushing requests out to tcp://127.0.0.1:9999.
Instead of connecting your handlers directly to this socket, run the
dispatcher device like so::

    python -m m2wsgi.device.dispatcher \
              tcp://127.0.0.1:9999
              tcp://127.0.0.1:8888

Then you can launch your handlers against the device's output socket and have
them chat with it about their availability.  Make sure you specify the conn
type to m2wsgi::

    m2wsgi --conn-type=Dispatcher dotted.app.name tcp://127.0.0.1:8888
           


OK, but why?
------------

In the standard PULL-based protocol, each handler process connects with a PULL
socket and requests are sent round-robin to all connected handlers.  This is
great for throughput but has some limitations:

  * there's no way to control which requests get routed to which handler,
    e.g. to implement sticky sessions or coordinate large uploads.
  * there's no way for a handler to cleanly disconnect - if it goes offline 
    with pending requests queued to it, those requests can get dropped.

In the XREQ-based protocol offered by this device, each socket instead connects
with a XREQ socket.  When it's ready for work, the handler sends an explicit
message identifying itself and we start pushing is requests.

To effect a clean disconnect, the handler can send a special disconnect message
(the single byte "X") and this device will flush any queued requests, then
respond with an empty request.  At this point the handler knows that no more
requests will be sent its way, and it can safely terminate.

The basic version of this device just routes reqeusts round-robin, but you
can easily implement more complex logic e.g. consistent hashing based on
a session token.


Any Downsides?
--------------

Yes, a little.  The device is not notified when handlers die unexpectedly,
so it will keep sending them requests which are silently dropped by zmq.

To mitigate this the device periodically resets its list of available
handlers, forcing them to re-contact the device for more work.  By default
this timer triggers every second (using SIGALRM).

Still working on a way to have the best of both worlds, but zmq doesn't want
to deliver any disconnection info to userspace.  The underlying socket has
a list of connected handlers at any given time, it just won't tell me
about it :-(

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


class Dispatcher(object):
    """Device for dispatching requests to handlers."""

    def __init__(self,send_sock_m,recv_sock_m,
                      send_sock_h,recv_sock_h,ping_sock_h,
                      heartbeat_interval=1):
        self.send_sock_m = send_sock_m
        self.recv_sock_m = recv_sock_m
        self.send_sock_h = send_sock_h
        self.recv_sock_h = recv_sock_h
        self.ping_sock_h = ping_sock_h
        self.heartbeat_interval = heartbeat_interval
        self.pending_requests = deque()
        self.pending_responses = deque()
        self.active_handlers = self.init_active_handlers()
        self.disconnecting_handlers = CheckableQueue()
        self.alarm_state = 0

    def run(self):
        """Run the socket handling loop."""
        signal.signal(signal.SIGALRM,self.alarm)
        while True:
            (r,_) = self.poll()
            #  When the alarm goes, forget all handlers.
            #  They have to prove they're still alive to get more work.
            if self.alarm_state == 2:
                self.clear_active_handlers()
                self.alarm_state = 3
            #  If we've just had an alarm and there is work to do,
            #  send out a ping for handlers.
            if self.alarm_state == 3:
                if self.pending_requests or self.send_sock_h in r:
                    if self.send_ping():
                        self.alarm_state = 0
            #  If we have active handlers, make sure an alarm is set.
            #  This lets us keep the heartbeat going without waking up
            #  all the time when there's nothing to do.
            if self.alarm_state == 0:
                if self.active_handlers:
                    signal.alarm(self.heartbeat_interval)
                    self.alarm_state = 1
            #  Disconnect any handlers that are waiting for it.
            if self.disconnecting_handlers:
                self.send_disconnect_messages()
            #  Forward any response data back to mongrel2.
            if self.recv_sock_h in r or self.pending_responses:
                self.forward_response_data()
            #  Handle any messages from handlers.  They might be
            #  coming back alive, or wanting to disconnect.
            if self.send_sock_h in r:
                self.read_handler_messages()
            #  If we have some active handlers, we can dispatch a request.
            #  Note that we only send a single request then re-enter
            #  this loop, to give handlers a chance to wake up.
            if self.active_handlers:
                if self.send_sock_m in r or self.pending_requests:
                    req = self.get_pending_request()
                    if req is not None:
                        try:
                            if not self.dispatch_request(req):
                                self.pending_requests.append(req)
                        except Exception:
                            self.pending_requests.append(req)
                            raise

    def poll(self):
        """Get the sockets that are ready for work.

        Which sockets we poll depends on what state we're in.
        We don't want to e.g. constantly wake up due to pending
        requests when we don't have any handlers to deal with them.
        """
        #  Always poll for new messages or responses from handlers
        rsocks = [self.send_sock_h,self.recv_sock_h]
        wsocks = []
        try:
            #  Poll for new requests if we have handlers ready, or if
            #  we have no pending requests.
            if self.active_handlers or not self.pending_requests:
                rsocks.append(self.send_sock_m)
            #  Poll for ability to send requests if we have some queued
            if self.pending_requests:
                wsocks.append(self.send_sock_m)
            #  Poll for ability to send shutdown acks if we have some queued
            if self.disconnecting_handlers:
                if self.send_sock_m not in wsocks:
                    wsocks.append(self.send_sock_m)
            #  Poll for writability of ping socket if we must ping
            if self.alarm_state == 3:
                if self.pending_requests:
                    wsocks.append(self.ping_sock_h)
            #  OK, we can now actually poll.
            (r,w,_) = zmq.core.poll.select(rsocks,wsocks,[])
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,):
                raise
            return ([],[])
        else:
            if e:
                raise RuntimeError("sockets have errored out")
            return (r,w)

    def alarm(self,sig,frm):
        """Handler for SIGALRM.

        This simply sets self.alarm_state to note that the alarm has
        gone off.  The signal will also interrupt self.poll() and then
        we can deal with the alarm in the main run loop.
        """
        self.alarm_state = 2

    def init_active_handlers(self):
        """Initialise and return the container for active handlers.

        By default this is a CheckableQueue object.  Subclasses can override
        this method to use a different container datatype.
        """
        return CheckableQueue()

    def clear_active_handlers(self):
        """Clear the list of active handlers.

        This forces all handlers to re-check-in with the dispatcher before
        they get sent any more requests.

        Subclasses may need to override this if they are using a custom
        container type for the active handlers, and it doesn't have a 
        clear() method.
        """
        self.active_handlers.clear()

    def send_ping(self):
        """Send a ping to all listening handlers.

        This asks them to check in with the dispatcher, so we can get their
        address and start sending them requests.
        """
        try:
            self.ping_sock_h.send("",zmq.NOBLOCK)
            return True
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise
            return False

    def send_disconnect_messages(self):
        """Send disconnection messages to anyone who needs it.

        This will give the handler a chance to either report back that
        it's still alive, or terminate cleanly.
        """
        try:
            handler = None
            while True:
                handler = self.disconnecting_handlers.popleft()
                self.send_sock_h.send(handler,zmq.SNDMORE|zmq.NOBLOCK)
                self.send_sock_h.send("",zmq.SNDMORE|zmq.NOBLOCK)
                self.send_sock_h.send("",zmq.NOBLOCK)
                handler = None
        except zmq.ZMQError, e:
            if handler is not None:
                self.disconnecting_handlers.append(handler)
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise
        except IndexError:
            pass
        
    def read_handler_messages(self):
        """Read messages coming in from handlers.

        This might be heartbeat messages letting us know the handler is
        still alive, or explicit disconnect messages.
        """
        try:
            while True:
                handler = self.send_sock_h.recv(zmq.NOBLOCK)
                delim = self.send_sock_h.recv(zmq.NOBLOCK)
                assert delim == "", "non-empty msg delimiter: "+delim
                msg = self.send_sock_h.recv(zmq.NOBLOCK)
                #  A diconnect message
                if msg == "X":
                    self.mark_handler_disconnecting(handler)
                #  A heartbeat message
                else:
                    self.mark_handler_active(handler)
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise

    def mark_handler_disconnecting(self,handler):
        """Mark the given handler as disconncting.

        We'll try to send it a disconnect message as soon as possible.
        """
        self.disconnecting_handlers.append(handler)
        try:
           self.active_handlers.remove(handler)
        except ValueError:
            pass

    def mark_handler_active(self,handler):
        """Mark the given handler as active.

        This means we can dispatch requests to this handler and have a
        reasonable chance of them being handled.
        """
        self.active_handlers.append(handler)
        try:
           self.disconnecting_handlers.remove(handler)
        except ValueError:
            pass

    def get_pending_request(self):
        """Get a pending request, or None is there's nothing pending."""
        if self.pending_requests:
            return self.pending_requests.popleft()
        try:
            return self.send_sock_m.recv(zmq.NOBLOCK)
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise
        return None

    def dispatch_request(self,req):
        """Dispatch a single request to an active handler.

        The default implementation iterates throught the handlers in a
        round-robin fashion.  For more sophisticated routing logic you
        might override this method to do e.g. consistent hashing based
        on a session cookie.

        Returns True if the request was successfully dispatched, False
        otherwise (and the dispatcher will keep it in memory to try again).
        """
        try:
            while True:
                handler = self.active_handlers.popleft()
                try:
                    self.send_sock_h.send(handler,zmq.SNDMORE|zmq.NOBLOCK)
                    self.send_sock_h.send("",zmq.SNDMORE|zmq.NOBLOCK)
                    self.send_sock_h.send(req,zmq.NOBLOCK)
                except zmq.ZMQError, e:
                    if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                        raise
                else:
                    return True
                finally:
                    self.active_handlers.append(handler)
        except IndexError:
            return False

    def forward_response_data(self):
        """Forward response data from handlers back to mongrel2.

        This method reads messages off the handler recv socket and sends
        them directly to the mongrel2 recv socket.  The only real trick
        is to keep a queue of unhandled responses in memory, in case we
        have to block mid-response for whatever reason.
        """
        try:
            while True:
                if self.pending_responses:
                    resp = self.pending_responses.popleft()
                else:
                    resp = None
                    handler = self.recv_sock_h.recv(zmq.NOBLOCK)
                    delim = self.recv_sock_h.recv(zmq.NOBLOCK)
                    assert delim == "", "non-empty msg delimiter: "+delim
                    resp = self.recv_sock_h.recv(zmq.NOBLOCK)
                self.recv_sock_m.send(resp,zmq.NOBLOCK)
                resp = None
        except zmq.ZMQError, e:
            if resp is not None:
                if self.pending_responses:
                    self.pending_responses.appendleft(resp)
                else:
                    self.pending_responses.append(resp)
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise


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

