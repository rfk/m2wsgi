"""

m2wsgi.device.reaper:  helper for timing out hung requests
==========================================================


This is incredibly simplistic, as the data from the control port isn't
very comprehensive.  Also it doesn't really work yet...


"""

import zmq
import json

CTX = zmq.Context()

def reaper(timeout=30):
    cp = CTX.socket(zmq.REQ)
    cp.connect("ipc://run/control")
    cp.send("status net")
    conns = json.loads(cp.recv())
    for (k,v) on conns:
        if v[1] > timeout:
            cp.send("kill " + str(k))
            cp.recv()

if __name__ == "__main__":
    reaper()
