"""

m2wsgi.middleware:  useful middleware for m2wsgi
================================================

This module contains helper middleware for building a good server.  It's mostly
things that are frowned upon in normal WSGI middleware, but are permitted when
you're the actual server.

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.


from m2wsgi.middleware.gzip import GZipMiddleware
from m2wsgi.middleware.buffer import BufferMiddleware

