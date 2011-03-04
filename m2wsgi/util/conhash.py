"""

m2wsgi.util.conhash:  classes for consistent hashing
====================================================

This module implements some simple classes for consistent hashing, to be
used for implementing "sticky sessions" in the dispatcher device.

The main interface is the class "ConsistentHash", which implements the
mapping interface to assign any given key to one of a set of targets.
Importantly, the target assigned to most keys will not change as targets
are added or removed from the hash.

Basic usage::

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


"""

import sys
import os
import hashlib
import bisect
import time

try:
    import scipy.stats
    import numpy
except ImportError:
    scipy = numpy = None


def md5hash(key):
    """Hash the object based on the md5 of its str().

    This is more expensive than the builtin hash() function but tends
    to shuffle better.  Python's builtin hash() has lots of special cases
    that make sense for dictionaries, but not for our purposes here.
    """
    return hashlib.md5(str(key)).hexdigest()[:8]
    return int(hashlib.md5(str(key)).hexdigest()[:8],16)


class RConsistentHash(object):
    """A consistent hash object based on arranging hashes in a ring.

    A ConsistentHash object maps arbitrary items onto one of a set of
    pre-specified targets, in such a way that adding or removing targets
    leaves the mapping of "most" of the items unchanged.

    This implementation is the one found in the paper introducing consistent
    hashing, and on wikipedia.  The hash values for the targets are arranged
    to form a closed ring.  To assign a key to a target, hash it to find its
    place on the ring then move clockwise until you find a target.

    This is nice from an efficiency standpoint as you only have to hash
    the key once, and the lookup is logarithmic in the number of targets.
    On the downside, it can be tricky to ensure uniformity of the target
    selection.  You must include many duplicates of each target to get an
    even distribution around the ring, and the right number to include will
    depend on the expected number of targets and the nature of your hash.
    """
    def __init__(self,hash=md5hash,num_duplicates=10000):
        self.target_ring = []
        self.hash = hash
        self.num_duplicates = num_duplicates

    def add_target(self,target):
        for i in xrange(self.num_duplicates):
            h = self.hash((i,target,))
            bisect.insort(self.target_ring,(h,target))

    def rem_target(self,target):
        self.target_ring = [(h,t) for (h,t) in self.target_ring if t != target]

    def has_target(self,target):
        for (h,t) in self.target_ring:
            if t == target:
                return True
        return False

    def __getitem__(self,key):
        h = self.hash(key)
        i = bisect.bisect_left(self.target_ring,(h,key))
        if i == len(self.target_ring):
            try:
                return self.target_ring[0][1]
            except IndexError:
                raise KeyError
        else:
            return self.target_ring[i][1]

    def __len__(self):
        return len(self.target_ring) / self.num_duplicates


class SConsistentHash(object):
    """A consistent hash object based on sorted hashes with each key.

    A ConsistentHash object maps arbitrary items onto one of a set of
    pre-specified targets, in such a way that adding or removing targets
    leaves the mapping of "most" of the items unchanged.

    This implementation is the one used by the Tahoe-LAFS project for server
    selection.  The key is hashed with each target and the resulting hashes
    are sorted.  The target producing the smallest hash is selected.

    What's nice about this implementation is that, assuming your hash function
    mixed well, it provides a very good uniform distribution without any
    tweaking.  It also uses much less memory than the RConsistentHash.
    On the downside, lookup is linear in the number of targets and you must
    call the hash function multiple times for each key.
    """
    def __init__(self,hash=md5hash):
        self.targets = set()
        self.hash = hash

    def add_target(self,target):
        self.targets.add(target)

    def rem_target(self,target):
        self.targets.remove(target)

    def has_target(self,target):
        return (target in self.targets)

    def __getitem__(self,key):
        targets = iter(self.targets)
        try:
            best_t = targets.next()
        except StopIteration:
            raise KeyError
        best_h = self.hash((key,best_t))
        for t in targets:
            h = self.hash((key,t))
            if h < best_h:
                best_t = t
                best_h = h
        return best_t

    def __len__(self):
        return len(self.targets)


#  For now, we use the SConsistentHash as standard.
#  It's slower but not really *slow*, and it consistently produces
#  a better uniform distribution of keys.
ConsistentHash = SConsistentHash


#
#  Some basic statistical tests for the hashes.
#  We're interested in:
#     * uniformity of distribution
#     * number of keys moved when a target is added/removed
#     * runtime performance
#

if numpy is not None:

    def iterpairs(seq):
        seq = iter(seq)
        try:
            first = seq.next()
            while True:
                second = seq.next()
                yield (first,second)
                first = second
        except StopIteration:
            pass

    def test_map(map):
        stats = get_map_stats(map)
        print "runtime:", "%.4f" % (stats[-1]["runtime"],)
        print "range:", stats[-1]["range"]
        print "stddev:", stats[-1]["stddev"]
        print "stdv/mean:", stats[-1]["stddev"] / stats[-1]["mean"] * 100
        for (first,second) in iterpairs(stats):
            res1 = first["results"]
            res2 = second["results"]
            nmoved = 0
            for k in res1:
                if res1[k] != res2[k]:
                    nmoved += 1
            pmoved = nmoved * 100.0 / len(res1)
            ntargets = len(second["counts"])
            print "added target, now ", ntargets, "total"
            print "should move around %.2f%% of keys" % (100.0 / ntargets,)
            print "moved %d of %d keys (%.2f%%)" % (nmoved,len(res1),pmoved,)
        if map_is_uniform(map,stats=stats):
            print "UNIFORM"
        else:
            print "BIASED"
        

    def map_is_uniform(map,p=0.01,stats=None):
        """Do a little chi-squared test for uniformity."""
        n = 4
        N = 100000
        E = N*1.0/n
        if stats is None:
            stats = get_map_stats(map,n,N)
        for s in stats:
            if scipy.stats.chisquare(s["counts"].values())[1] <= p:
                return False
        else:
            return True

    def get_map_stats(map,n=4,N=100000):
        """Gather some statistics by exercising the given map."""
        #  Randomly generate n+2 different targets
        res = [{},{},{}]
        stats = [{},{},{}]
        for i in xrange(n+2):
            t = os.urandom(4).encode("hex")
            try:
                while map[t] == t:
                    t = os.urandom(4).encode("hex")
            except (KeyError,IndexError,):
                pass
            map.add_target(t)
        #  For each of n, n+1, n+2, lookup N randomly-generated keys.
        #  We start at n+2, then remove a target for each iteration.
        keys = [os.urandom(16).encode("hex") for _ in xrange(N)]
        for i in reversed(xrange(3)):
            tstart = time.time()
            for k in keys:
                res[i][k] = map[k]
            tend = time.time()
            stats[i]["results"] = res[i]
            counts = {}
            for (k,t) in res[i].iteritems():
                counts[t] = counts.get(t,0) + 1
            stats[i]["counts"] = counts
            stats[i]["mean"] = sum(counts.values()) / len(counts)
            stats[i]["range"] = max(counts.values())-min(counts.values())
            stats[i]["stddev"] = numpy.std(counts.values())
            stats[i]["runtime"] = tend - tstart
            if i:
                t = res[i][keys[0]]
                map.rem_target(t)
        return stats

    
    if __name__ == "__main__":
        print "Testing ConsistentHash..."
        test_map(RConsistentHash())
        print "Testing SConsistentHash..."
        test_map(SConsistentHash())


