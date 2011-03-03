"""

m2wsgi.device.reaper:  helper for timing out requests
=====================================================


This is a device for timing out hung or expired requests.  It uses the mongrel2
control port to query connection status and issue kill commands, so you have
to have the control port open for it to work.

Run it like this::

    python -m m2wsgi.device.reaper \
              --max-lifetime=120   \
              ipc://run/control

This will poll the mongrel2 control port looking for connections that are
more than 2 minutes old.  If it finds any, it terminates them with extreme
prejudice.

By default the reaper device stays alive and repeatedly polls for connections.
To run a single cleanup sweep and then exit, specify the --oneshot option.

Current timeout options include max connection lifetime and max idle time.
You will need to be running a post-1.5 version of mongrel2 to use the idle
time option.

"""

import json
import time
from textwrap import dedent

import zmq
from m2wsgi.io.standard import Connection


class Reaper(object):

    def __init__(self,ctrl_sock,recv_sock=None,**opts):
        if isinstance(ctrl_sock,basestring):
            ctrl_sock = Connection.makesocket(zmq.REQ,ctrl_sock)
        if isinstance(recv_sock,basestring):
            recv_sock = Connection.makesocket(zmq.PUB,recv_sock)
        self.stopped = False
        self.ctrl_sock = ctrl_sock
        self.recv_sock = recv_sock
        self.max_lifetime = opts.pop("max_lifetime",60)
        self.max_idle_time = opts.pop("max_idle_time",30)
        if opts:
            raise TypeError("unknown options: %s" % (opts.keys(),))

    def run(self):
        next_reap_time = 0
        while not self.stopped:
            time.sleep(next_reap_time)
            next_reap_time = self.reap()

    def stop(self):
        self.stopped = True

    def reap(self):
        next_reap_time = min(self.max_lifetime,self.max_idle_time)
        self.ctrl_sock.send("status net")
        conns = json.loads(self.ctrl_sock.recv())
        for (k,v) in conns.iteritems():
            if k == "total":
                continue
            #  Older mongrel2 returned a list.  Newer
            #  versions return a dict with more info.
            if isinstance(v,list):
                v = dict(fd=v[0],last_ping=v[1])
            if self.max_lifetime is not None:
                last_ping = v.get("last_ping",0)
                if last_ping >= self.max_lifetime:
                    self.ctrl_sock.send("kill " + str(k))
                    self.ctrl_sock.recv()
                    continue
                time_left = self.max_lifetime - last_ping
                if time_left < next_reap_time:
                    next_reap_time = time_left
            if self.max_idle_time is not None:
                last_activity = min(v.get("last_read",0),
                                    v.get("last_write",0))
                if last_activity >= self.max_idle_time:
                    self.ctrl_sock.send("kill " + str(k))
                    self.ctrl_sock.recv()
                    continue
                time_left = self.max_idle_time - last_activity
                if time_left < next_reap_time:
                    next_reap_time = time_left
        return next_reap_time


if __name__ == "__main__":
    import optparse
    op = optparse.OptionParser(usage=dedent("""
    usage:  m2wsgi.device.reaper [options] [ctrl_spec] [recv_spec]
    """))
    op.add_option("","--max-lifetime",type="int",default=60,
                  help="max connection lifetime in seconds")
    op.add_option("","--max-idle-time",type="int",default=30,
                  help="max idle connection time in seconds")
    op.add_option("","--oneshot",action="store_true",
                  help="only reap once, then exit")
    (opts,args) = op.parse_args()
    recv_sock = None
    if not args:
        ctrl_sock = "ipc://run/control"
    elif len(args) == 1:
        ctrl_sock = args[0]
    elif len(args) == 2:
        ctrl_sock = args[0]
        recv_sock = args[1]
    else:
         raise ValueError("reaper expects at most two arguments")
    oneshot = opts.__dict__.pop("oneshot")
    r = Reaper(ctrl_sock,recv_sock,**opts.__dict__)
    if oneshot:
        r.reap()
    else:
        r.run()

