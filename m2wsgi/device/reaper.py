"""

m2wsgi.device.reaper:  helper for timing out hung requests
==========================================================


"""

import zmq

CTX = zmq.Context()


def reaper():
    cp = CTX.socket(zmq.REQ)
    cp.connect("ipc://run/control")
    cp.send("time")
    print cp.recv()
    cp.send("status tasks")
    print cp.recv()
    cp.send("status net")
    print cp.recv()

if __name__ == "__main__":
    reaper()
