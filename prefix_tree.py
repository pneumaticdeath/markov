#!/usr/bin/python

# Copyright 2009, Mitch Patenaude

__author__ = 'Mitch Patenaude (patenaude@gmail.com)'

import random 
import tree

class MarkovChain:
  """A markov implementation that uses a hash-map for the first m elements, and
  devolves to a tree thereafter.

  """

  def __init__(self, max=3, min=None, _suffix_class=tree.MarkovChain):
    """Build a new Markov chain object.

    Instansiates a new MarkovChain object that will collect stats.

    Arguments:
     max: maximum length of tuple to keep stats for. (optional, default = 3) 
    """
    self._max=max
    if min is None:
      self._min = max - 1
    else:
      if min >= max or min < 1:
        raise ValueError('valid values for minimum chain length are 1 <= min < max')
      self._min = min
    self._max_key=self._min-1
    self.count=0
    self._tuple_map = {}
    self._suffix_class = _suffix_class

  def Update(self, seq):
    """Updates from a tuple or list, but not an interator."""
    # this takes care of all the full length subsequences,
    # plus the final sequences >= min in length
    for ind in xrange(len(seq)-self._min+2):
      self._UpdateTuple(tuple(seq[ind:ind+self._max]))


  def _UpdateTuple(self, t):
    """Updates the statistics for this tuple.

    Updates the statistics of this chain wih the supplied tuple.

    Arguments:
      t: a tuple of strings or other elements

    Retrns:
      Nothing
    """

    self.count += 1

    if len(t) == 0:
      return


    key = t[:self._max_key]
    if len(t) <= self._max_key:
      element = tuple()
    else:
      element = t[self._max_key:]

    if key not in self._tuple_map:
      self._tuple_map[key] = self._suffix_class(max=(self._max-self._max_key), min=1)
    self._tuple_map[key]._UpdateTuple(element)

  def GetRandomTuple(self, seed=None, depth=None):

    if depth is None:
      depth=self._max
    elif depth > self._max or depth < 1:
      raise ValueError("invalid depth %d: must be between 1 and %d" % (depth, self._max))

    if seed is not None and len(seed) > self._max_key:
      key_seed = seed[:self._max_key]
      suffix_seed = seed[self._max_key:]
    else:
      key_seed = seed
      suffix_seed = None
    
    key = self._GetRandomKey(key_seed)

    if not key:
      return tuple()

    if len(key) >= depth:
      return key[:depth]

    suffix_tuple = self._tuple_map[key].GetRandomTuple(seed=suffix_seed, depth=(depth-self._max_key))

    return key+suffix_tuple

  def _GetRandomKey(self,seed=None):
    """Get a random n-tuple key based on the seed provided.

    Arguments:
      seed: (optional) a seed tuple

    Returns:
      a tuple based on the seed and the distribution in the chain

    """

    if seed and len(seed) >= self._max_key:
      # then there isn't much to choose..
      return seed

    if seed is None:
      cand_tuples = self._tuple_map.keys()
      total = self.count
    else:
      seed_len = len(seed)
      cand_tuples = filter(lambda tup: tup[0:seed_len] == seed,
                           self._tuple_map.keys())
      total = sum([self._tuple_map[tup].count for tup in cand_tuples])
    if not cand_tuples:
      # there aren't any matching tuples...
      # should this raise a KeyError?
      return None
    target_count = random.uniform(0,total)
    for tup in cand_tuples:
      leaf = self._tuple_map[tup]
      if target_count < leaf.count:
        return tup
      else:
        target_count -= leaf.count
    # This shouldn't be possible... since this is how we counted them up.
    raise RuntimeError("The total was more than the sum of it's parts: "
                       "%d remaining after %d counted" % (target_count,total))

  def GetRandomSequence(self,seed=None, depth=None):
    """Generate a random sequence of elements.

    Returns a generator which will return a random sequence
    (with an optional seed sequence.) The seqence will end when the 
    chain hits a likely stoping point.  This might be never if the
    tree has never been seeded with a sequence that never has an
    endpoint.

    Arguments:
      seed: a tuple to use as the seed of the sequence

    Returns:
      An iterable sequence of elements.
    """

    if depth is None:
      depth = self._max

    seq = self.GetRandomTuple(seed, depth=depth)

    # If the sequence is less than the full length we asked for, then 
    # it means we've reached a natual stopping point and the loop should
    # end.
    while len(seq) >= depth:
      yield seq[0]
      seq = self.GetRandomTuple(seed=seq[1:], depth=depth)

    # play out the rest of the sequence 
    for element in seq:
      yield element

  def PrintTree(self, depth=None, _rec_depth=0):
    raise NotImplementedError()

if __name__ == "__main__":
  pass
