#!/usr/bin/python

# Copyright 2009, Mitch Patenaude

__author__ = 'Mitch Patenaude (patenaude@gmail.com)'

import random 
from limited_types import simple_set as set
from limited_types import simple_tuple as tuple
from limited_types import simple_list as list
from limited_types import simple_dict as dict
# from limited_types import list_dict as dict

class _MCLeaf(object):
  """Leaf node on the markov tree
  
  This class contains the count of final elements, and an overall count.
  """
  
  __slots__ = ['count','labels','elem_labels','elem']

  def __init__(self, element, label=None):
    self.count=1
    self.labels = set()
    self.elem_labels = dict()
    self.elem = dict()
    if element is not None:
      self.elem[element] = 1
      if label is not None:
        self.elem_labels[element] = set([label,])
      else:
        self.elem_labels[element] = set()
    elif label is not None:
      self.labels.add(label)

  def Update(self, element, label=None):
    """Update the count, ignore the rest.

    Arguments:
      element: The element in this leaf to be updated (None for terminating tuples)
      label: optional label to apply to this update.

    """
    self.count += 1

    if element is None:
      if label is not None:
        self.labels.add(label)
      return

    if element in self.elem:
      self.elem[element] += 1
    else:
      self.elem[element] = 1
      self.elem_labels[element] = set()

    if label is not None:
      self.elem_labels[element].add(label)

  def GetRandomElement(self, labelset=None):
    target_count = random.uniform(0,self.count)
    for (element,element_count) in self.elem.items():
      if target_count < element_count:
        if labelset is not None:
          labelset |= self.elem_labels[element]
        return element
      else :
        target_count -= element_count
    if labelset is not None:
      labelset |= self.labels
    return None

class MarkovChain(object):
  """A hash-map representation of a markov chain.

  A class which will keep track of n-tuple frequencies, where fixed 
  lookup map of size (max-1) is used.
  """

  def __init__(self, max=3):
    """Build a new Markov chain object.

    Instansiates a new MarkovChain object that will collect stats.

    Arguments:
     max: maximum length of tuple to keep stats for. (optional, default = 3) 
    """
    self._max_key=max-1
    self._max=max
    self.count=0
    self._tuple_map = dict()

  def Update(self, seq, label=None):
    """Updates from a tuple or list, but not an interator."""
    # this takes care of all the full length subsequences
    for ind in xrange(len(seq)-self._max+1):
      self._UpdateTuple(tuple(seq[ind:ind+self._max]), label=label)

    # And this puts in the end condition, and also works if the sequence
    # was shorter than the maximum to begin with.
    self._UpdateTuple(tuple(seq[-(self._max-1):]), label=label)

  def _UpdateTuple(self, t, label=None):
    """Updates the statistics for this tuple.

    Updates the statistics of this chain wih the supplied tuple.

    Arguments:
      t: a tuple of strings or other elements

    Retrns:
      Nothing
    """

    if len(t) == 0:
      return

    self.count += 1

    key = t[:self._max_key]
    if len(t) <= self._max_key:
      element = None
    else:
      element = t[self._max_key]

    if key in self._tuple_map:
      self._tuple_map[key].Update(element, label=label)
    else:
      self._tuple_map[key] = _MCLeaf(element, label=label)

  def GetRandomTuple(self, seed=None, depth=None, labelset=None):

    key = self._GetRandomKey(seed)

    if not key:
      return tuple()

    element = self._tuple_map[key].GetRandomElement(labelset=labelset)

    if element:
      key += tuple([element,])

    if depth is not None and len(key) > depth:
      return key[:depth]
    else:
      return key

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
      raise KeyError('No candidate tuples match seed %s' % (repr(seed),))
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

  def _GetNext(self, tuple, labelset=None):
    return self._tuple_map[tuple].GetRandomElement(labelset=labelset)

  def GetRandomSequence(self,seed=None, depth=None, labelset=None):
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

    # TODO(mitch):
    # the basic logic for depth implementation is
    # seq = GetRandomTuple(seed)
    # while True:
    #   yeild seq[0]
    #   seq = seq[1:]
    #   if len(seq) < depth
    #     seq = self._GetRandomKey(seed=seq)
    #     next = self._GetNext(seq)
    #   if next:
    #     seq += tuple(next)
    #   else:
    #     # We hit a stopping point.
    #     break
    # for remaining in seq
    #   yeild remaining
    #     

    # Build an initial sequence based on the provided seed.
    seq = self._GetRandomKey(seed)

    full_seq_len = self._max_key
    # If the sequence is less than the full length we asked for, then 
    # it means we've reached a natual stopping point and the loop should
    # end.
    while len(seq) >= full_seq_len:
      yield seq[0]
      next_element = self._GetNext(seq, labelset=labelset)

      seq = seq[1:]
      if next_element:
        seq += (next_element,)

    # play out the rest of the sequence 
    for element in seq:
      yield element

  def PrintTree(self, depth=None, _rec_depth=0):
    raise NotImplementedError()

if __name__ == "__main__":
  pass
