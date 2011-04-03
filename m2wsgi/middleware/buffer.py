"""

m2wsgi.middleware.buffer:  response buffering middleware
========================================================

This is a WSGI middleware class that buffers the response iterable so it 
doesn't return lots of small chunks.

It is completely and totally against the WSGI spec, which requires that the
server not delay transmission of any data to the client.  But if you don't care 
about this requirement, you can use this middelware to improve the performance
of e.g. content gzipping.

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.


from __future__ import absolute_import


class BufferMiddleware(object):
    """WSGI middleware for buffering response content.

    This is completely and totally against the WSGI spec, which requires that
    the server not delay transmission of any data to the client.  But if you
    don't care about this requirement, you can use this middelware to improve
    the performance of e.g. content gzipping.

    Supports the following keyword arguments:

        :min_chunk_size:     minimum size of chunk to yield up the chain

    """

    def __init__(self,application,**kwds):
        self.application = application
        self.min_chunk_size = kwds.pop("min_chunk_size",200)

    def __call__(self,environ,start_response):
        output = self.application(environ,start_response)
        return BufferIter(output,self.min_chunk_size)


class BufferIter(object):
    """Iterator wrapper buffering chunks yielded by another iterator."""

    def __init__(self,iterator,min_chunk_size):
        self.iterator = iter(iterator)
        self.min_chunk_size = min_chunk_size
        self.buffer = []

    def __iter__(self):
        return self

    def __len__(self):
        #  We don't know how long the iterator is, with one exception.
        #  If it's only a single item, then so are we.
        nitems = len(self.iterator)
        if nitems > 1:
            raise TypeError
        return nitems

    def next(self):
        min_size = self.min_chunk_size
        try:
            size = 0
            while size < min_size:
                self.buffer.append(self.iterator.next())
                size += len(self.buffer[-1])
            chunk = "".join(self.buffer)
            del self.buffer[:]
            return chunk
        except StopIteration:
            if not self.buffer:
                raise
            else:
                chunk = "".join(self.buffer)
                del self.buffer[:]
                return chunk

