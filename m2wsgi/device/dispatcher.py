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
message identifying itself and we start pushing it requests.

To effect a clean disconnect, the handler can send a special disconnect message
(the single byte "X") and this device will flush any queued requests, then
respond with an "X" request.  At this point the handler knows that no more
requests will be sent its way, and it can safely terminate.

The basic version of this device just routes reqeusts round-robin, but you
can easily implement more complex logic e.g. consistent hashing based on
a session token.


Any Downsides?
--------------

Yes, a little.  The device is not notified when handlers die unexpectedly,
so it will keep sending them requests which are silently dropped by zmq.

To mitigate this the device sends periodic "ping" signals out via a PUB
socket.  Handlers that don't respond to a ping within a certain amount of
time are considered dead and dropped.  By default the ping triggers every
second.

Still working on a way to have the best of both worlds, but zmq doesn't want
to deliver any disconnection info to userspace.  The underlying socket has
a list of connected handlers at any given time, it just won't tell me
about it :-(

"""

import os
import errno
import threading
from textwrap import dedent
from collections import deque

import zmq.core.poll

from m2wsgi.io.standard import Connection
from m2wsgi.util import CheckableQueue


class Dispatcher(object):
    """Device for dispatching requests to handlers."""

    def __init__(self,send_sock,recv_sock,disp_sock,ping_sock,ping_interval=1):
        self.running = False
        if isinstance(send_sock,basestring):
            send_sock = Connection.makesocket(zmq.PULL,send_sock)
        if isinstance(recv_sock,basestring):
            recv_sock = Connection.makesocket(zmq.PUB,recv_sock)
        if isinstance(disp_sock,basestring):
            disp_sock = Connection.makesocket(zmq.XREP,disp_sock)
        if isinstance(ping_sock,basestring):
            ping_sock = Connection.makesocket(zmq.PUB,ping_sock)
        self.send_sock = send_sock
        self.recv_sock = recv_sock
        self.disp_sock = disp_sock
        self.ping_sock = ping_sock
        self.ping_interval = ping_interval
        self.pending_requests = deque()
        self.pending_responses = deque()
        self.active_handlers = self.init_active_handlers()
        #  Handlers that have been sent a ping and haven't yet sent a
        #  reply are "dubious".  Handlers that have sent a disconnect
        #  signal but haven't been sent a reply are "disconnecting".
        #  Anything else is "alive".
        self.dubious_handlers = set()
        self.disconnecting_handlers = CheckableQueue()
        self.alive_handlers = set()
        #  The state of our ping cycle.
        #  0:  quiescent; no requests, all handlers dubious
        #  1:  need to send a new ping
        #  2:  ping sent, ping timeout alarm pending
        #  3:  ping timeout alarm fired, needs action
        self.ping_state = 0
        #  We implement the ping timer by having a background thread
        #  write into this pipe for each 'ping'.  This avoids having
        #  to constantly query the current time and calculate timeouts.
        #  We could do this with SIGALRM but it would preclude using the
	#  device as part of a larger program.
        (self.ping_pipe_r,self.ping_pipe_w) = os.pipe()
        self.ping_thread = None
        self.ping_cond = threading.Condition()

    def close(self):
        """Shut down the device, closing all its sockets."""
        #  Stop running, waking up the select() if necessary.
        self.running = False
        os.write(self.ping_pipe_w,"X")
        #  Close the ping pipe, then shut down the background thread.
        (r,w) = (self.ping_pipe_r,self.ping_pipe_w)
        (self.ping_pipe_r,self.ping_pipe_w) = (None,None)
        os.close(r)
        os.close(w)
        if self.ping_thread is not None:
            with self.ping_cond:
                self.ping_cond.notify()
            self.ping_thread.join()
        #  Now close down all our zmq sockets
        self.ping_sock.close()
        self.disp_sock.close()
        self.send_sock.close()
        self.recv_sock.close()

    def _run_ping_thread(self):
        """Background thread that periodically wakes up the main thread.

        This code is run in a background thread.  It periodically wakes
        up the main thread by writing to self.ping_pipe_w.  We do things
        this way so that the main thread doesn't have to constantly query
        the current time and calculate timeouts.
        """
        #  Each time the main thread wants to trigger a ping, it will
        #  notify on self.ping_cond to wake us up.  Otherwise we'd
        #  never go to sleep when no requests are coming in.
        with self.ping_cond:
            while self.running:
                self.ping_cond.wait()
                #  Rather than using time.sleep(), we do a select-with-timeout
                #  on the ping pipe so that any calls to close() will wake us
                #  up immediately.
                socks = [self.ping_pipe_r]
                if socks[0] is None:
                    break
                zmq.core.poll.select(socks,[],socks,timeout=self.ping_interval)
                self._trigger_ping_alarm()
                self.ping_state = 3
                                     
    def _trigger_ping_alarm(self):
        """Trigger the ping alarm by writing to self.ping_pipe_w."""
        w = self.ping_pipe_w
        if w is not None:
            try:
                os.write(w,"P")
            except EnvironmentError:
                pass

    def _whatis(self,sock):
        for (k,v) in self.__dict__.iteritems():
            if sock == v:
                return k
        return "???"

    def run(self):
        """Run the socket handling loop."""
        self.running = True
        #  Send an initial ping, to activate any handlers that have
        #  already started up before us.
        while not self.send_ping() and self.running:
            zmq.core.select.poll([self.ping_pipe_r],[self.ping_sock],[])
        #  Run the background thread that interrupts us whenever we
        #  need to process a ping.
        self.ping_thread = threading.Thread(target=self._run_ping_thread)
        self.ping_thread.start()
        #  Enter the dispatch loop.
        #  Any new handlers that come online will introduce themselves
        #  by sending us an empty message, or at the very least will
        #  be picked up on the next scheduled ping.
        while self.running:
            ready = self.poll()
            #print "READY", self.ping_state, map(self._whatis,ready), list(self.active_handlers)
            #  If we're quiescent and there are requests ready, go
            #  to active pinging of handlers.
            if self.ping_state == 0:
                if self.send_sock in ready or self.pending_requests:
                    self.ping_state = 1
            #  If we need to send a ping but there's no work to do, go
            #  quiescent instead of waking everyone up.
            elif self.ping_state == 1:
                if not self.active_handlers:
                    self.ping_state = 0
                elif not self.alive_handlers:
                    if self.send_sock not in ready:
                        if not self.pending_requests:
                            self.ping_state = 0
            #  If we need to send a ping, try to do so.  This might fail
            #  if the socket isn't ready for it, so we retry on next iter.
            #  If successful, schedule a ping alarm.
            if self.ping_state == 1:
                if self.send_ping():
                    with self.ping_cond:
                        self.ping_state = 2
                        self.ping_cond.notify()
            #  When the ping alarm goes, any dubious handlers get dropped
            #  and any alive handlers get marked as dubious.  We will send
            #  them a new ping message and they must respond before the next
            #  ping alarm to stay active.
            if self.ping_state == 3:
                os.read(self.ping_pipe_r,1)
                for handler in self.dubious_handlers:
                    try:
                        self.rem_active_handler(handler)
                    except ValueError:
                        pass
                self.dubious_handlers = self.alive_handlers
                self.alive_handlers = set()
                self.ping_state = 1
            #  Disconnect any handlers that are waiting for it.
            if self.disconnecting_handlers:
                self.send_disconnect_messages()
            #  Forward any response data back to mongrel2.
            if self.disp_sock in ready or self.pending_responses:
                self.read_handler_responses()
            #  If we have some active handlers, we can dispatch a request.
            #  Note that we only send a single request then re-enter
            #  this loop, to give handlers a chance to wake up.
            if self.active_handlers or not self.pending_requests:
                req = self.get_pending_request()
                if req is not None:
                    try:
                        if not self.dispatch_request(req):
                            self.pending_requests.append(req)
                    except Exception:
                        self.pending_requests.append(req)
                        raise

    def poll(self):
        """Get the sockets that are ready for reading.

        Which sockets we poll depends on what state we're in.
        We don't want to e.g. constantly wake up due to pending
        requests when we don't have any handlers to deal with them.
        """
        #  Always poll for new responses from handlers, and
        #  for ping timeout alarms.
        rsocks = [self.disp_sock,self.ping_pipe_r]
        wsocks = []
        try:
            #  Poll for new requests if we have handlers ready, or if
            #  we have no pending requests.
            if self.active_handlers or not self.pending_requests:
                rsocks.append(self.send_sock)
            #  Poll for ability to send requests if we have some queued
            if self.pending_requests and self.active_handlers:
                wsocks.append(self.disp_sock)
            #  Poll for ability to send shutdown acks if we have some queued
            if self.disconnecting_handlers:
                if self.disp_sock not in wsocks:
                    wsocks.append(self.disp_sock)
            #  Poll for ability to send responses if we have some pending
            if self.pending_responses:
                wsocks.append(self.recv_sock)
            #  Poll for writability of ping socket if we must ping
            if self.ping_state == 1:
                wsocks.append(self.ping_sock)
            #  OK, we can now actually poll.
            #print "POLL", self.ping_state, map(self._whatis,rsocks), map(self._whatis,wsocks), list(self.active_handlers)
            (ready,_,_) = zmq.core.poll.select(rsocks,wsocks,[])
            return ready
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,):
                raise
            return []

    def init_active_handlers(self):
        """Initialise and return the container for active handlers.

        By default this is a CheckableQueue object.  Subclasses can override
        this method to use a different container datatype.
        """
        return CheckableQueue()

    def rem_active_handler(self,handler):
        """Remove the given handler from the list of active handlers.

        Subclasses may need to override this if they are using a custom
        container type for the active handlers, and it doesn't have a 
        remove() method.
        """
        self.active_handlers.remove(handler)

    def add_active_handler(self,handler):
        """Add the given handler to the list of active handlers.

        Subclasses may need to override this if they are using a custom
        container type for the active handlers, and it doesn't have an
        append() method.
        """
        self.active_handlers.append(handler)

    def send_ping(self):
        """Send a ping to all listening handlers.

        This asks them to check in with the dispatcher, so we can get their
        address and start sending them requests.  It might fail is the
        ping socket isn't ready; returns bool indicating success.
        """
        try:
            self.ping_sock.send("",zmq.NOBLOCK)
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
                self.disp_sock.send(handler,zmq.SNDMORE|zmq.NOBLOCK)
                self.disp_sock.send("",zmq.SNDMORE|zmq.NOBLOCK)
                self.disp_sock.send("X",zmq.NOBLOCK)
                try:
                    self.rem_active_handler(handler)
                except ValueError:
                    pass
                handler = None
        except zmq.ZMQError, e:
            if handler is not None:
                self.disconnecting_handlers.append(handler)
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise
        except IndexError:
            pass
        
    def read_handler_responses(self):
        """Read responses coming in from handlers.

        This might be heartbeat messages letting us know the handler is
        still alive, explicit disconnect messages, or response data to
        forward back to mongrel2.
        """
        try:
            while True:
                if self.pending_responses:
                    resp = self.pending_responses.popleft()
                    handler = None
                else:
                    resp = None
                    handler = self.disp_sock.recv(zmq.NOBLOCK)
                    delim = self.disp_sock.recv(zmq.NOBLOCK)
                    assert delim == "", "non-empty msg delimiter: "+delim
                    resp = self.disp_sock.recv(zmq.NOBLOCK)
                if resp == "X":
                    self.mark_handler_disconnecting(handler)
                else:
                    if handler is not None:
                        self.mark_handler_alive(handler)
                    if resp:
                        self.recv_sock.send(resp,zmq.NOBLOCK)
                    resp = None
        except zmq.ZMQError, e:
            if resp is not None:
                self.pending_responses.appendleft(resp)
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise

    def mark_handler_disconnecting(self,handler):
        """Mark the given handler as disconncting.

        We'll try to send it a disconnect message as soon as possible.
        """
        self.disconnecting_handlers.append(handler)
        try:
           self.alive_handlers.remove(handler)
        except KeyError:
            pass
        try:
           self.dubious_handlers.remove(handler)
        except KeyError:
            pass

    def mark_handler_alive(self,handler):
        """Mark the given handler as alive.

        This means we can dispatch requests to this handler and have a
        reasonable chance of them being handled.
        """
        if handler not in self.active_handlers:
            self.add_active_handler(handler)
        self.alive_handlers.add(handler)
        try:
           self.dubious_handlers.remove(handler)
        except KeyError:
            pass
        try:
           self.disconnecting_handlers.remove(handler)
        except ValueError:
            pass

    def get_pending_request(self):
        """Get a pending request, or None is there's nothing pending."""
        if self.pending_requests:
            return self.pending_requests.popleft()
        try:
            return self.send_sock.recv(zmq.NOBLOCK)
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
                    return self.send_request_to_handler(req,handler)
                finally:
                    self.active_handlers.append(handler)
        except IndexError:
            return False

    def send_request_to_handler(self,req,handler):
        """Send the given request to the given handler."""
        try:
            self.disp_sock.send(handler,zmq.SNDMORE|zmq.NOBLOCK)
            self.disp_sock.send("",zmq.SNDMORE|zmq.NOBLOCK)
            self.disp_sock.send(req,zmq.NOBLOCK)
            return True
        except zmq.ZMQError, e:
            if e.errno not in (errno.EINTR,zmq.EAGAIN,):
                raise
            return False



if __name__ == "__main__":
    import optparse
    op = optparse.OptionParser(usage=dedent("""
    usage:  m2wsgi.device.dispatcher send_spec recv_spec disp_spec ping_spec
    """))
    (opts,args) = op.parse_args()
    d = Dispatcher(*args)
    try:
        d.run()
    finally:
        d.close()
