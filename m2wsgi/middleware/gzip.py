"""

m2wsgi.middleware.gzip:  GZip content-encoding middleware
=========================================================

This is a GZip content-encoding WSGI middleware.  It strives hard for WSGI
compliance (including e.g. streaming of data chunks).  Unless you tell it
not to bother.

"""


from __future__ import absolute_import

import gzip
from cStringIO import StringIO
from itertools import chain


class GZipMiddleware(object):
    """WSGI middleware for gzipping response content.

    Yeah yeah, don't do that, it's the province of the server.  Well, this
    is a server and an outer-layer middleware object seems like the simlest
    way to achieve it.

    Unlike every other gzipping-middleware I have ever seen (except a diff
    that's languished in Django's bugzilla for 3 years) this one is capable
    of compressing streaming responses.  It's also capable of violating the
    WSGI spec in return for performance, if that's your thing.

    Supports the following keyword arguments:

        :compresslevel:      gzip compression level; default is 9

        :min_compress_size:  don't bother compressing data smaller than this,
                             unless the spec requires it; default is 200 bytes

        :break_wsgi_compliance:  break WSGI compliance if it means we can
                                 compress more efficiently; default is False

    """

    def __init__(self,application,**kwds):
        self.application = application
        self.compresslevel = kwds.pop("compresslevel",9)
        self.min_compress_size = kwds.pop("min_compress_size",200)
        self.break_wsgi_compliance = kwds.pop("break_wsgi_compliance",False)
        #  There are many other ways to express how you feel
        #  about certain aspects of WSGI...
        for prefix in ("ignore","screw","shpx".decode("rot13"),):
            key = prefix + "_wsgi_compliance"
            self.break_wsgi_compliance |= kwds.pop(key,False)

    def __call__(self,environ,start_response):
        handler = []
        def my_start_response(status,headers,exc_info=None):
            if not self._should_gzip(environ,status,headers):
                handler.append(self._respond_uncompressed)
                return start_response(status,headers,exc_info)
            else:
                gzf = gzip.GzipFile(mode="wb",fileobj=StringIO(),
                                    compresslevel=self.compresslevel)
                has_content_length = False
                for (i,(k,v)) in enumerate(headers):
                    if k.lower() == "content-length":
                        has_content_length = True
                    elif k.lower() == "vary":
                        if "accept-encoding" not in v.lower():
                            if v:
                                headers[i] = (k,v+", Accept-Encoding")
                            else:
                                headers[i] = (k,"Accept-Encoding")
                if has_content_length:
                    handler.append(self._respond_compressed_block)
                    handler.append(gzf)
                    handler.append(start_response)
                    handler.append(status)
                    handler.append(headers)
                    handler.append(exc_info)
                else:
                    start_response(status,headers,exc_info)
                    handler.append(self._respond_compressed_stream)
                    handler.append(gzf)
                #  The returned write() function will buffer all of its
                #  data until an actual chunk is yielded from the app.
                return gzf.write
        output = iter(self.application(environ,my_start_response))
        #  We have to read up to the first yielded chunk before branching,
        #  to give the app a change to call start_response.
        try:
            (_,output) = ipeek(output)
        except StopIteration:
            output = [""]
        return handler[0](output,*handler[1:])

    def _respond_uncompressed(self,output):
        """Respond with raw uncompressed data; the easy case."""
        for chunk in output:
            yield chunk

    def _respond_compressed_stream(self,output,gzf):
        """Respond with a stream of compressed chunks.

        This is pretty easy, but you have to mind the WSGI requirement to
        always yield each chunk in full whenever the application yields a
        chunk.  Unless you don't care of course...
        """
        if self.break_wsgi_compliance:
            for chunk in output:
                if chunk:
                    gzf.write(chunk)
                    if gzf.fileobj.tell() >= self.min_compress_size:
                        gzf.flush()
                        yield gzf.fileobj.getvalue()
                        gzf.fileobj = StringIO()
        else:
            for chunk in output:
                if not chunk:
                    yield chunk
                else:
                    gzf.write(chunk)
                    gzf.flush()
                    yield gzf.fileobj.getvalue()
                    gzf.fileobj = StringIO()
        gzf.close()
        yield gzf.fileobj.getvalue()

    def _respond_compressed_block(self,output,gzf,sr,status,headers,exc_info):
        """Respond with a single block of compressed data.

        Since this method will have to adjust the final content-length header,
        it maintains responsibility for calling start_response.

        Note that we can only maintain the WSGI requirement that we not delay
        any blocks if the application output provides a __len__ method and it           returns 1.  Otherwise, we have to *remove* the Content-Length header
        and stream the response, then let the server sort out how to terminate
        the connection.
        """
        #  Helper function to remove an content-length headers and
        #  then respond with streaming compression.
        def streamit():
            todel = []
            for (i,(k,v)) in headers:
                if k.lower() == "content-length":
                    todel.append(i)
            for i in reversed(todel):
                del headers[i]
            sr(status,headers,exc_info)
            return self._respond_compressed_stream(output,gzf)
        #  Check if we can safely compress the whole body.
        if not self.break_wsgi_compliance:
            try:
                num_chunks = len(output)
            except Exception:
                return streamit()
            else:
                if num_chunks > 1:
                    return streamit()
        #  OK, we can compress it all in one go.
        for chunk in output:
            gzf.write(chunk)
        gzf.close()
        body = gzf.getvalue()
        for (i,(k,v)) in headers:
            if k.lower() == "content-length":
                headers[i] = (k,str(len(body)))
        sr(status,headers,exc_info)
        return [body]

    def _should_gzip(self,environ,status,headers):
        """Determine whether we should bother gzipping.

        This checks whether the client will accept it, or whether it just
        seems like a bad idea.
        """
        code = status.split(" ",1)[0]
        #  Don't do it if the browser doesn't support it.
        if gzip not in environ.get("HTTP_ACCEPT_ENCODING",""):
            return False
        #  Don't do it for error responses, or things with no content.
        if not code.startswith("2"):
            return False
        if code in ("204",):
            return False
        #  Check various response headers
        for (k,v) in headers:
            #  If it's also content-encoded, must preserver
            if k.lower() == "content-encoding":
                return False
            #  If it's too small, don't bother
            if k.lower() == "content-length":
                try:
                    if int(v) < self.min_compress_size:
                        return False
                except Exception:
                    return False
            #  As usual, MSIE has issues
            if k.lower() == "content-type":
                if "msie" in environ.get("HTTP_USER_AGENT","").lower():
                    if not v.strip().startswith("text/"):
                        return False
                    if "javascript" in v:
                        return False
        return True


def ipeek(iterable):
    """Peek at the first item of an iterable.

    This returns a two-tuple giving the first item from the iteration,
    and a stream yielding all items from the iterable.  Use it like so:

        (first,iterable) = ipeek(iterable)

    If the iterable is empty, StopItertation is raised.
    """
    firstitem = iterable.next()
    return (firstitem,_PeekedIter(firstitem,iterable))

class _PeekedIter(object):
    """Iterable that has had its first item peeked at."""
    def __init__(self,firstitem,iterable):
        self.iterable = iterable
        self.allitems = chain(firstitem,iterable)
    def __len__(self):
        return len(self.iterable)
    def __iter__(self):
        return self
    def next(self):
        return self.allitems.next()
        
