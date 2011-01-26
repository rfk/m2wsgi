"""

m2wsgi.util:  miscellaneous utility functions
=============================================

The standard kind of "util" module you'd expect from any python package.
Take a look around, but there's not a lot to see...

"""

import re
from urllib import unquote


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


def pop_netstring(data):
    """Pop a netstring from the front of the given data.

    This function pops a netstring-formatted chunk of data from the front
    of the given string.  It returns a two-tuple giving the contents of the
    netstring and any remaining data.
    """
    (size,body) = data.split(':', 1)
    size = int(size)
    if body[size] != ",":
        raise ValueError("not a netstring: %r" % (data,))
    return (body[:size],body[size+1:])

def encode_netstring(data):
    """Encode the given data as a netstring."""
    return "%d:%s," % (len(data),data,)

def encode_netstrings(datas):
    """Encode the given sequence of data strings as joined netstrings."""
    return "".join(encode_netstring(d) for d in datas)


_QUOTED_SLASH_RE = re.compile("(?i)%2F")
def unquote_path(path):
    """Unquote each component of a url-encoded path."""
    bits = _QUOTED_SLASH_RE.split(path)
    return "%2F".join(unquote(bit) for bit in bits)


class InputStream(object):
    """An abstract base class for implementing WSGI input streams.

    You provide an implementation of the _read() method; this class provides
    all the bits and bobs required for a WSGI input stream.
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


