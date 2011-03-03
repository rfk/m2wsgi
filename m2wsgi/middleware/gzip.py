"""

m2wsgi.middleware.gzip:  GZip content-encoding middleware
=========================================================

This is a GZip content-encoding WSGI middleware.  It strives hard for WSGI
compliance, even at the expense of compression.  If you want to trade off
compliance for better compression, put an instance of BufferMiddleware inside
it to collect small chunks and compress them all at once.

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.


from __future__ import absolute_import

import gzip
from cStringIO import StringIO
from itertools import chain


class GZipMiddleware(object):
    """WSGI middleware for gzipping response content.

    Yeah yeah, don't do that, it's the province of the server.  Well, this
    is a server and an outer-layer middleware object seems like the simplest
    way to implement gzip compression support.

    Unlike every other gzipping-middleware I have ever seen (except a diff
    that's languished in Django's bugzilla for 3 years) this one is capable
    of compressing streaming responses.  But beware, if your app yields lots
    of small chunks this will probably *increase* the size of the payload
    due to the overhead of the compression data.

    This class supports the following keyword arguments:

        :compress_level:      gzip compression level; default is 9

        :min_compress_size:  don't bother compressing data smaller than this,
                             unless the spec requires it; default is 200 bytes

    """

    def __init__(self,application,**kwds):
        self.application = application
        self.compress_level = kwds.pop("compress_level",9)
        self.min_compress_size = kwds.pop("min_compress_size",200)

    def __call__(self,environ,start_response):
        handler = []
        #  We can't decide how to properly handle the response until we
        #  have the headers, which means we have to do this in the 
        #  start_response function.  It will append a callable and
        #  any required arguments to the 'handler' list.
        def my_start_response(status,headers,exc_info=None):
            if not self._should_gzip(environ,status,headers):
                handler.append(self._respond_uncompressed)
                return start_response(status,headers,exc_info)
            else:
                gzf = gzip.GzipFile(mode="wb",fileobj=StringIO(),
                                    compresslevel=self.compress_level)
                #  We must stream the chunks if there's no content-length
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
                headers.append(("Content-Encoding","gzip"))
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
                #  This is the stupid write() function required by the spec.
                #  It will buffer all data written until a chunk is yielded
                #  from the application.
                return gzf.write
        output = self.application(environ,my_start_response)
        #  We have to read up to the first yielded chunk to give
        #  the app a change to call start_response.
        try:
            (_,output) = ipeek(iter(output))
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
        chunk.  Throw in some buffering middleware if you don't care about
        this requirement, it'll make compression much better.
        """
        for chunk in output:
            if not chunk:
                yield chunk
            else:
                gzf.write(chunk)
                gzf.flush()
                yield gzf.fileobj.getvalue()
                gzf.fileobj = StringIO()
        fileobj = gzf.fileobj
        gzf.close()
        yield fileobj.getvalue()

    def _respond_compressed_block(self,output,gzf,sr,status,headers,exc_info):
        """Respond with a single block of compressed data.

        Since this method will have to adjust the final content-length header,
        it maintains responsibility for calling start_response.

        Note that we can only maintain the WSGI requirement that we not delay
        any blocks if the application output provides a __len__ method and it           returns 1.  Otherwise, we have to *remove* the Content-Length header
        and stream the response, then let the server sort out how to terminate
        the connection.
        """
        #  Helper function to remove any content-length headers and
        #  then respond with streaming compression.
        def streamit():
            todel = []
            for (i,(k,v)) in enumerate(headers):
                if k.lower() == "content-length":
                    todel.append(i)
            for i in reversed(todel):
                del headers[i]
            sr(status,headers,exc_info)
            return self._respond_compressed_stream(output,gzf)
        #  Check if we can safely compress the whole body.
        #  If not, stream it a chunk at a time.
        try:
            num_chunks = len(output)
        except Exception:
            return streamit()
        else:
            if num_chunks > 1:
                return streamit()
        #  OK, we can compress it all in one go.
        #  Make sure to adjust content-length header.
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
        if "gzip" not in environ.get("HTTP_ACCEPT_ENCODING",""):
            return False
        #  Don't do it for error responses, or things with no content.
        if not code.startswith("2"):
            return False
        if code in ("204",):
            return False
        #  Check various response headers
        for (k,v) in headers:
            #  If it's already content-encoded, must preserve
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
        self.allitems = chain((firstitem,),iterable)
    def __len__(self):
        return len(self.iterable)
    def __iter__(self):
        return self
    def next(self):
        return self.allitems.next()
        
