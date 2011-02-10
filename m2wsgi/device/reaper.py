"""

m2wsgi.device.reaper:  helper for timing out requests
==========================================================


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

This device is currently very simplistic, as the data from the control port
isn't very comprehensive.  It supports only a max connection lifetime.

I hope to enhance mongrel2's control port with more information about each
connection (e.g. time since last write, number of bytes read) and then beef
up the timeout options offered by this device.


"""

import json
import time
from textwrap import dedent

import zmq
from m2wsgi.io.standard import Connection

def reaper(ctrl_sock,max_lifetime=60,oneshot=False):
    if isinstance(ctrl_sock,basestring):
        ctrl_sock = Connection.makesocket(zmq.REQ,ctrl_sock)
    next_reap_time = 0
    while True:
        time.sleep(next_reap_time)
        next_reap_time = max_lifetime
        ctrl_sock.send("status net")
        conns = json.loads(ctrl_sock.recv())
        for (k,v) in conns.iteritems():
            if k == "total":
                continue
            (fd,last_ping) = v
            if last_ping >= max_lifetime:
                ctrl_sock.send("kill " + str(k))
                ctrl_sock.recv()
            else:
                time_left = max_lifetime - last_ping
                if time_left < next_reap_time:
                    next_reap_time = time_left
        if oneshot:
            break

if __name__ == "__main__":
    import optparse
    op = optparse.OptionParser(usage=dedent("""
    usage:  m2wsgi.device.reaper [options] [ctrl_spec]
    """))
    op.add_option("","--max-lifetime",type="int",default=60,
                  help="max connection lifetime in seconds")
    op.add_option("","--oneshot",action="store_true",
                  help="only reap once, then exit")
    (opts,args) = op.parse_args()
    if not args:
        ctrl_sock = "ipc://run/control"
    elif len(args) > 1:
         raise ValueError("reaper expects a single argument")
    else:
        ctrl_sock = args[0]
    reaper(ctrl_sock,**opts.__dict__)

