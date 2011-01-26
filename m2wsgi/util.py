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
    """Unquote a url-encoded path."""
    bits = _QUOTED_SLASH_RE.split(path)
    return "%2F".join(unquote(bit) for bit in bits)

