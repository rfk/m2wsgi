
import sys
import zmq

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

    while True:
        req = in_sock.recv()
        worker = out_sock.recv()
        delimiter = out_sock.recv()
        readymsg = out_sock.recv()
        out_sock.send(worker,zmq.SNDMORE)
        out_sock.send("",zmq.SNDMORE)
        out_sock.send(encode_netstrings((req,)))

if __name__ == "__main__":
    push2queue("tcp://127.0.0.1:9999","tcp://127.0.0.1:9989","XXX","YYY")
