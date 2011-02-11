"""

m2wsgi.util.conhash:  simple class for consistent hashing
=========================================================

This module provides the class ConsistentHash, a simple class for doing
routing based on consistent hashing.  Use it like this::

   ch = ConsistentHash()

   #  add some targets, which items will map to
   ch.add_target("ONE")
   ch.add_target("TWO")
   ch.add_target("THREE")

   #  now you can map items one one of the assigned targets
   assert ch["hello"] == "THREE"
   assert ch["world"] == "ONE"

   #  and removing targets leaves most mappings unchanged
   ch.rem_target("ONE")
   assert ch["hello"] == "THREE"
   assert ch["world"] == "THREE"


Note that the current implementation seems to have a systematic bias; it
distributes items in a manner that is definitely not uniform.  I can't find
the source of the problem, yet...

"""

import os
import hashlib
import bisect

try:
    from scipy import stats
    import numpy
except ImportError:
    stats = numpy = None

def md5hash(key):
    """Hash the object based on the md5 of its str().

    This is more expensive than the builtin hash() function but tends
    to shuffle better (or is it just my imagination?)
    """
    return hashlib.md5(str(key)).hexdigest()


class ConsistentHash(object):
    """An object for consistent hashing.

    A ConsistentHash object maps arbitrary items onto one of a set of
    pre-specified targets, in such a way that adding or removing targets
    changes the mapping of only a few items.
    """
    def __init__(self,default_weight=50,hash=md5hash):
        self.target_ring = []
        self.hash = hash
        self.default_weight = default_weight

    def add_target(self,target,weight=None):
        if weight is None:
            weight = self.default_weight
        for i in xrange(weight):
            h = self.hash((i,target,))
            bisect.insort(self.target_ring,(h,target))

    def rem_target(self,target):
        self.target_ring = [(h,t) for (h,t) in self.target_ring if t != target]

    def __getitem__(self,key):
        h = self.hash(key)
        i = bisect.bisect_left(self.target_ring,(h,key))
        if i == len(self.target_ring):
            return self.target_ring[0][1]
        else:
            return self.target_ring[i][1]


if stats is not None:
    def is_it_uniform(map,p=0.01):
        """Do a little chi-squared test for uniformity."""
        n = 4
        N = 100000
        E = N*1.0/n
        for i in xrange(n):
            map.add_target(os.urandom(4).encode("hex"))
        counts = {}
        for i in xrange(N):
            t = map[os.urandom(16)]
            if t not in counts:
                counts[t] = 1
            else:
                counts[t] += 1
        if stats.chisquare(counts.values())[1] > p:
            print "yes, it's uniform."
            return True
        else:
            print "no, it's biased."
            print "counts:", counts
            print "range:", max(counts.values()) - min(counts.values())
            print "stddev:", numpy.std(counts.values())

    
    if __name__ == "__main__":
        ch = ConsistentHash()
        print "is it uniform?"
        is_it_uniform(ch)


