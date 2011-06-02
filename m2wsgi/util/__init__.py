"""

m2wsgi.util:  miscellaneous utility functions
=============================================

The standard kind of "util" module you'd expect from any python package.
Low-level data format handlers, helper functions, that kind of thing...

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.


import sys
import os
import re
from urllib import unquote
from collections import deque


def fix_absolute_import(filename):
    """Fix broken absolute imports when running submodules as a script.

    So here's the problem.  When you run things as scripts, python likes to
    put the current directory in your sys.path.  If anything in there has
    the same name as a top-level python module, it masks that module and you
    get broken imports.  Like this::

        #> cd m2wsgi/
        #> python eventlet.py
        Traceback (most recent call last):
          File "eventlet.py", line 20, in <module>
            import eventlet.hubs
          File "<...>/m2wsgi/eventlet.py", line 20, in <module>
            import eventlet.hubs
        ImportError: No module named hubs
        #>

    Here python is trying to import the m2wsgi.eventlet module as the top-level
    eventlet module, and this obviously doesn't work!  To fix it, put this
    at the top of eventlet.py::

        from __future__ import absolute_import
        from m2wsgi.util import fix_absolute_import
        fix_absolute_import(__file__)

    This will ensure that the submodule's parent directory isn't on sys.path,
    so there's no possibility of python importing it as something else by
    mistake.
    """
    if not sys.path:
        return
    syspath0 = os.path.realpath(sys.path[0])
    if syspath0 == os.path.dirname(os.path.realpath(filename)):
        del sys.path[0]


def load_dotted_name(name):
    """Load an object, giving its full dotted name.

    Currently this isn't very smart, e.g. it can't load attributes off an
    object.  I'll improve it as I need it.
    """
    try:
        (modnm,objnm) = name.rsplit(".",1)
    except ValueError:
        return __import__(name,fromlist=["*"])
    else:
        mod = __import__(modnm,fromlist=["*"])
        return getattr(mod,objnm)



def pop_tnetstring(string):
    """Pop a tnetstring from the front of the given data.

    This function pops a tnetstring-formatted chunk of data from the front
    of the given string.  It returns a two-tuple giving the value encoded
    in the tnetstring and any remaining data.
    """
    #  Parse out data length, type and remaining string.
    try:
        (dlen,rest) = string.split(":",1)
        dlen = int(dlen)
    except ValueError:
        raise ValueError("not a tnetstring: missing or invalid length prefix")
    try:
        (data,type,remain) = (rest[:dlen],rest[dlen],rest[dlen+1:])
    except IndexError:
        #  This fires if len(rest) < dlen, meaning we don't need
        #  to further validate that data is the right length.
        raise ValueError("not a tnetstring: invalid length prefix")
    #  Parse the data based on the type tag.
    if type == ",":
        return (data,remain)
    if type == "#":
        if "." in data or "e" in data or "E" in data:
            try:
                return (float(data),remain)
            except ValueError:
                raise ValueError("not a tnetstring: invalid float literal")
        else:
            try:
                return (int(data),remain)
            except ValueError:
                raise ValueError("not a tnetstring: invalid integer literal")
    if type == "!":
        if data == "true":
            return (True,remain)
        elif data == "false":
            return (False,remain)
        else:
            raise ValueError("not a tnetstring: invalid boolean literal")
    if type == "~":
        if data:
            raise ValueError("not a tnetstring: invalid null literal")
        return (None,remain)
    if type == "]":
        l = []
        while data:
            (item,data) = pop_tnetstring(data)
            l.append(item)
        return (l,remain)
    if type == "}":
        d = {}
        while data:
            (key,data) = pop_tnetstring(data)
            (val,data) = pop_tnetstring(data)
            d[key] = val
        return (d,remain)
    raise ValueError("unknown type tag")

#
#  Try to use c-accelerated tnetstrings if available.
#
try:
    from tnetstring import pop as pop_tnetstring
except ImportError:
    pass


_QUOTED_SLASH_RE = re.compile("(?i)%2F")
def unquote_path(path):
    """Unquote each component of a url-encoded path."""
    bits = _QUOTED_SLASH_RE.split(path)
    return "%2F".join(unquote(bit) for bit in bits)


class InputStream(object):
    """An abstract base class for implementing WSGI input streams.

    You provide an implementation of the _read() method; this class provides
    all the bits and bobs required for a WSGI input stream.  It's a stipped
    down version of the FileLikeBase class from my filelike module:

        http://www.rfk.id.au/software/filelike/

    """

    def __init__(self):
        self._rbuffer = None

    def _read(self,sizehint=-1):
        """Read approximately <sizehint> bytes from the file-like object.
        
        This method is to be implemented by subclasses.  It should read
        approximately 'sizehint' bytes from the file and return them as
        a string.  If 'sizehint' is missing, negative or None then try
        to read all remaining contents.
        
        This method need not guarantee any particular number of bytes -
        it may return more bytes than requested, or fewer.  If needed, the
        size hint may be completely ignored.  It may even return an empty
        string if no data is yet available.
        
        Because of this, the method must return None to signify that EOF
        has been reached.  The higher-level methods will never indicate EOF
        until None has been read from _read().  Once EOF is reached, it
        must be safe to call _read() again, immediately returning None.
        """
        raise NotImplementedError

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc_val,exc_tb):
        self.close()
        return False

    def __iter__(self):
        return self

    def next(self):
        """next() method complying with the iterator protocol.

        File-like objects are their own iterators, with each call to
        next() returning subsequent lines from the file.
        """
        ln = self.readline()
        if ln == "":
            raise StopIteration()
        return ln

    def read(self,size=None):
        """Read at most 'size' bytes from the file.

        Bytes are returned as a string.  If 'size' is negative, None or
        missing, the remainder of the file is read.  If EOF is encountered
        immediately, the empty string is returned.
        """
        if size is None or size < 0:
            #  Read through to end of file.
            if self._rbuffer:
                data = [self._rbuffer]
                self._rbuffer = ""
            else:
                data = []
            new_data = self._read()
            while new_data is not None:
                data.append(new_data)
                new_data = self._read()
            data = "".join(data)
        else:
            #  Read a specific amount of data.
            if self._rbuffer:
                new_data = self._rbuffer
                data = [new_data]
            else:
                new_data = ""
                data = []
            size_so_far = len(new_data)
            while size_so_far < size:
                new_data = self._read(size-size_so_far)
                if new_data is None:
                    break
                data.append(new_data)
                size_so_far += len(new_data)
            data = "".join(data)
            if size_so_far > size:
                # read too many bytes, store in the buffer
                self._rbuffer = data[size:]
                data = data[:size]
            else:
                self._rbuffer = ""
        return data

    def readline(self,size=-1):
        """Read a line from the file, or at most 'size' bytes."""
        bits = []
        indx = -1
        size_so_far = 0
        while indx == -1:
            if size > 0:
                next_bit = self.read(min(1024,size - size_so_far))
            else:
                next_bit = self.read(1024)
            bits.append(next_bit)
            size_so_far += len(next_bit)
            if next_bit == "":
                break
            if size > 0 and size_so_far >= size:
                break
            indx = next_bit.find("\n")
        # If not found, return whole string up to 'size' length
        # Any leftovers are pushed onto front of buffer
        if indx == -1:
            data = "".join(bits)
            if size > 0 and size_so_far > size:
                extra = data[size:]
                data = data[:size]
                self._rbuffer = extra + self._rbuffer
            return data
        # If found, push leftovers onto front of buffer
        indx += 1  # preserve newline in return value
        extra = bits[-1][indx:]
        bits[-1] = bits[-1][:indx]
        self._rbuffer = extra + self._rbuffer
        return "".join(bits)

    def readlines(self,sizehint=-1):
        """Return a list of all lines in the file."""
        return [ln for ln in self]


class CheckableQueue(object):
    """A combined deque/set for O(1) queueing and membership checking.

    This class maintains both a deque and a set and provides the following
    operations in O(1) time:

        * membership testing
        * append-to-back
        * pop-from-front
        * item removal

    The only tricky one is item removal - for this to work in constant time
    the items must be removed from the set but left in the deque.  To prevent
    duplicates appearing in the deque if a removed item is re-inserted, we
    maintain a count of how often an item has been removed and suppress
    removed items when they show up at the front of the queue.
    """

    def __init__(self):
        self.__members = set()
        self.__removed_count = {}
        self.__queue = deque()

    def __contains__(self,item):
        return (item in self.__members)

    def __len__(self):
        #  NOT len(self.__queue) as it may contain duplicates.
        return len(self.__members)

    def __iter__(self):
        return iter(self.__members)

    def clear(self):
        self.__members.clear()
        self.__queue.clear()
        self.__removed.clear()

    def append(self,item):
        if item not in self.__members:
            self.__members.add(item)
            self.__queue.append(item)

    def popleft(self):
        #  Since items remain in the queue after removal, we
        #  must loop to find one that's still "alive".
        while True:
            item = self.__queue.popleft()
            rc = self.__removed_count.get(item,0)
            if not rc:
                #  The item is alive; return it.
                self.__members.remove(item)
                return item
            else:
                #  The item was removed; suppress it.
                rc -= 1
                if rc:
                    self.__removed_count[item] = rc
                else:
                    del self.__removed_count[item]

    def remove(self,item):
        try:
            self.__members.remove(item)
        except KeyError:
            raise ValueError(item)
        if item in self.__removed_count:
            self.__removed_count[item] += 1
        else:
            self.__removed_count[item] = 1


def force_ascii(value):
    """Force a value to contain only ascii strings, not unicode.

    This is mostly to bridge between Mongrel2's demand that everything be
    ascii and the json module's habit of outputting unicode.
    """
    if isinstance(value,unicode):
        return value.encode("ascii")
    if isinstance(value,list):
        return [force_ascii(v) for v in value]
    if isinstance(value,dict):
        items = value.iteritems()
        return dict((force_ascii(k),force_ascii(v)) for (k,v) in items)
    return value
 

