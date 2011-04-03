
import unittest

import os
import sys
import shutil

import m2wsgi
import m2wsgi.io.base
from m2wsgi import io

class TestMisc(unittest.TestCase):

    def test_README(self):
        """Ensure that the README is in sync with the docstring.

        This test should always pass; if the README is out of sync it just
        updates it with the contents of m2wsgi.__doc__.
        """
        dirname = os.path.dirname
        readme = os.path.join(dirname(dirname(dirname(__file__))),"README.rst")
        if not os.path.isfile(readme):
            f = open(readme,"wb")
            f.write(m2wsgi.__doc__.encode())
            f.close()
        else:
            f = open(readme,"rb")
            if f.read() != m2wsgi.__doc__:
                f.close()
                f = open(readme,"wb")
                f.write(m2wsgi.__doc__.encode())
                f.close()

class TestRequestParsing(unittest.TestCase):

    def test_parsing_netstring_payloads(self):
        fnm = os.path.join(os.path.dirname(__file__),"request_payloads.txt")
        json_reqs = []; tns_reqs = []
        with open(fnm,"r") as f:
            for i,ln in enumerate(f):
                if i % 2 == 0:
                    json_reqs.append(ln.strip())
                else:
                    tns_reqs.append(ln.strip())
        assert len(json_reqs) == len(tns_reqs)
        def parse_request(data):
            (server_id,client_id,rest) = data.split(" ",2)
            client = io.base.Client(None,server_id,client_id)
            return io.base.Request.parse(client,rest)
        for (r_json,r_tns) in zip(json_reqs,tns_reqs):
            self.assertEquals(parse_request(r_json),parse_request(r_tns))
