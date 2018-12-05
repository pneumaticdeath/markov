#!/usr/bin/python

# Copyright 2009,2018 Mitch Patenaude

__author__ = 'Mitch Patenaude (patenaude@gmail.com)'

from limited_types import simple_set as set
from limited_types import simple_dict as dict
# from limited_types import list_dict as dict
from limited_types import simple_tuple as tuple

import random 

class _MCLeaf(object):
  """Leaf node on the markov tree
  
  This class contains the count, but doesn't have the overhead of a dict
  class.
  
  The Update method merely increments the count.
  """

  _max = 0

  __slots__ = ['count', 'labels']

  def __init__(self):
    self.count=0
    self.labels = set()

  def _UpdateTuple(self, unused_seq, label=None, _labelExclDepth=0):
    """Update the count and labels, ignore the rest.

    Arguments:
      unused_seq: Unused sequence object to match #MarkovChain.Update()
    """

    if label is not None and _labelExclDepth <= 0:
      # depending on the uniqeness of the set to remove dupes
      self.labels.add(label)
    
    self.count += 1

  def GetRandomTuple(self, seed, depth, labelset=None):
    """ this should never be called """
    raise NotImplementedException()
    return tuple()

  def _GetLabels(self, seq):
    if seq:
      raise ValueError('MCLeaf can\'t contain subsequences like {}'.format(repr(seq)))
    return self.labels.copy()


class MarkovChain(dict):
  """A tree representation of a markov chain.

  A class which will keep track of n-tuple frequencies by building a counter tree, with a configuratble maximum tuple size.

  This implementation allows more flexible and quick lookup of substrees, but is much less memory efficient that the tuple_map
  implementation
  """

  __slots__ = ['count', '_max', '_min', 'labels' ]

  def __init__(self, max=3, min=None):
    """Build a new Markov chain object.

    Instansiates a new MarkovChain object that will collect stats.

    Arguments:
     max: maximum length of tuple to keep stats for. (optional, default = 3) 
     min: minimum length of tuple about which statistics will be updated by 
       the Update() method.  See warning in the GetRandomSequence() method.

    """
    dict.__init__(self)
    self.count=0
    self._max=max
    if min is None:
      min = max-1
    elif min > max:
      raise ValueError("minimum tuple size cannot exceed maximum")
    self._min = min
    self.labels = set()

  def Update(self, seq, label=None):
    """Updates from a tuple or list, but not an interator."""
    # this takes care of all the full length subsequences
    # for ind in xrange(len(seq)-self._min+1):
    for ind in xrange(len(seq)-self._min+1):
      self._UpdateTuple(tuple(seq[ind:ind+self._max]), label=label)

  def _UpdateTuple(self, t, label=None, _labelExclDepth=None):
    """updates the statistics.

    Updates the statistics of this chain wih the supplied tuple.

    Arguments:
      t: a tuple of strings or other elements
      label: A label to associate with this tuple
      _labelExclDepth: Exclude tuples of less than this depth, default = min

    Retrns:
      Nothing
    """

    self.count += 1

    if _labelExclDepth is None:
      _labelExclDepth=self._min

    if len(t) == 0 or self._max == 0:
      # if this is a terminating node, then add the label regardless of exclusion rules.
      if label is not None:
        self.labels.add(label)
      return
    elif _labelExclDepth <= 0 and label is not None:
      # we only want to add labels at nodes that might be terminating nodes
      self.labels.add(label)
      
    car = t[0]
    cdr = t[1:]

    if car not in self:
      if self._max == 1:
        self[car] = _MCLeaf()
      else:
        self[car] = MarkovChain(self._max-1) 
    self[car]._UpdateTuple(cdr, label=label, _labelExclDepth=_labelExclDepth-1)

  def GetRandomTuple(self, seed=None, depth=None, labelset=None):
    """Get a random n-tuple based on the seed provided.

    Arguments:
      seed: (optional) a seed tuple
      depth: (optional) integer value of the max depth into the tree of the tuple
      labelset: If not None, assumed to be a set of lables involved in this sequence, which will be updated.


    Returns:
      a tuple based on the seed and the distribution in the chain

    Raises: 
      ValueError: if depth > max for the whole tree
    """

    if depth is None:
      depth = self._max
    elif depth > self._max:
      raise ValueError("depth cannot exceed the tree depth")

    if depth == 0 or len(self) == 0:
      if labelset is not None:
        labelset |= self.labels
      return tuple()

    if seed:
      retVal = seed[0]
      subSeed = seed[1:]
    else:
      retVal = self._GetRandomElement()
      subSeed = None

    if not retVal or retVal not in self:
      # since we got a null back or the seed isn't found, we return
      if labelset is not None:
        # since there is nothing down the tree, we'll use this set of labels instead.
        labelset |= self.labels
      return tuple()

    # if we're at the bottom of the tree, we don't recurse,
    # otherwise we walk down the tree passing the labelset on.
    if depth <= 1:
      if labelset is not None:
        labelset |= self[retVal].labels
      return (retVal,)
    else:
    
      return (retVal,) + self[retVal].GetRandomTuple(seed=subSeed,
                                                     depth=depth-1,
                                                     labelset=labelset)

  def _GetRandomElement(self):
    """Find a random element.

    Returns:
      A random element weighted by the distribution, or None if the end condition is hit.
    """
    target = random.uniform(0,self.count)
    for (tw,mkv) in self.items():
      if target < mkv.count:
        return tw
      else:
        target -= mkv.count
    return None

  def GetRandomSequence(self, seed=None, depth=None, labelset=None):
    """Generate a random sequence of elements.

    Returns a generator which will return a random sequence
    (with an optional seed sequence.) The seqence will end when the 
    chain hits a likely stoping point.  This might be never if the
    tree has never been seeded with a sequence that never has an
    endpoint.

    Arguments:
      seed: a tuple to use as the seed of the sequence
      depth: The depth of the tree to use for the statistical weightings.
        The next element will be determined by the depth-1'th preceding 
        element in the chain.
      labelset: if not none, then it is assumed to be a set to keep track 
        of labels that went into making the sequence. Currently does not
        work for sequences where depth!=max

    Warnings:
      If depth < min+1 the termination point may not be realistic.
      If min == max there may not be a termination point, but there
        might be a KeyError if the sequence has hit a stopping point.

    Returns:
      An iterable sequence of elements.
    """

    if depth is not None:
      full_seq_len = depth
    else:
      full_seq_len = self._max

    if seed and len(seed) >= full_seq_len:
      # if the seed is already longer than a full_seq_length - 1
      # then we should just feed off the initial elements until
      # the sequence should be created normally
      excess = len(seed) - full_seq_len 
      for ind in xrange(0,excess):
        yield seed[ind]
      seq = seed[excess:]
    else:
      # Build an initial sequence based on the provided seed.
      seq = self.GetRandomTuple(seed,depth, labelset=labelset)

    # If the sequence is less than the full length we asked for, then 
    # it means we've reached a natual stopping point and the loop should
    # end.
    while len(seq) >= full_seq_len:
      yield seq[0]
      seq = seq[1:]
      new_seq = self.GetRandomTuple(seq, depth=depth, labelset=labelset)
      if new_seq:
        seq = new_seq

    # play out the rest of the sequence 
    for element in seq:
      yield element

  def _GetLabels(self, seq):
    if len(seq) > 0:
      if seq[0] in self:
        return self[seq[0]]._GetLabels(seq[1:])
      else:
        return None
    else:
      return self.labels.copy()

  def GetAnnotatedSequence(self, seed, depth=None):

    if depth is None:
      depth = self._max

    while seed and len(seed) >= depth:
      seq = seed[:depth]
      labelset = self._GetLabels(seq)
      yield seq[0], labelset
      seed = seed[1:]

    labelset = set()
    seq = self.GetRandomTuple(seed, depth=depth, labelset=labelset)
    while len(seq) >= depth:
      yield seq[0], labelset
      labelset = set()
      seq = self.GetRandomTuple(seq[1:], depth=depth, labelset=labelset)

    for element in seq:
      yield element, labelset

  def PrintTree(self, depth=None, _rec_depth=0):
    if depth is None:
      depth = self._max
    for wrd, mkv in self.items():
      print "%s%-20s %d" % (" " * _rec_depth, repr(wrd), mkv.count)
      if depth > 1:
        mkv.PrintTree(depth=depth-1,_rec_depth=_rec_depth+1)

if __name__ == "__main__":
  pass
