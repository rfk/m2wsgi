"""

m2wsgi.io:  I/O class definitions for m2wsgi
============================================

This package contains implementations of the base m2wsgi classes for various
I/O subsystems, e.g. using normal threads or using eventlet.  So far we have:

  * base:        abstract base classes, not useful on their own
  * standard:    standard I/O for use with blocking thread-based programs
  * eventlet:    classes designed for use with the eventlet main loop
  * gevent:      classes designed for use with the gevent main loop

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.

