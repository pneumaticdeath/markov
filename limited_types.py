#!/usr/bin/python

"""This is a set of low(er) overhead classes"""

class simple_set(set):
  __slots__ = []

class simple_dict(dict):
  __slots__ = []

class simple_list(list):
  __slots__ = []

class simple_tuple(tuple):
  __slots__ = []

class list_dict(object):
  """This is a memory efficient dict implementation.

Key/value pairs are kept sorted by key, so that lookups are log(n)."""

  __slots__ = ['_key_list', '_value_list']

  def __init__(self, init_dict=None):
    self._key_list = simple_list()
    self._value_list = simple_list()
    if init_dict:
      for (key, value) in init_dict.itemiter():
        self[key] = value

  def _slot_lookup(self, key):
    if len(self._key_list) == 0:
      return 0

    i,j = 0, len(self._key_list)
    while i < j:
      m = (i+j)/2
      if self._key_list[m] < key:
        i=m+1
      else:
        j=m

    return i

  def __setitem__(self, key, value):
    i = self._slot_lookup(key)

    if i == len(self._key_list):
      self._key_list.append(key)
      self._value_list.append(value)
    elif key == self._key_list[i]:
      self._value_list[i] = value
    else:
      self._key_list.insert(i, key)
      self._value_list.insert(i, value)

  def __getitem__(self, key):
    i = self._slot_lookup(key)
    if i < len(self._key_list) and self._key_list[i] == key:
      return self._value_list[i]
    else:
      raise KeyError('Unknown key %s' % str(key))

  def __delitem__(self, key):
    i = self._slot_lookup(key)
    if i < len(self._key_list) and self._key_list[i] == key:
      del self._key_list[i]
      del self._value_list[i]
    else:
      raise KeyError('Unknown key %s' % str(key))

  def __contains__(self, key):
    i = self._slot_lookup(key)
    return i < len(self._key_list) and key == self._key_list[i]

  def __len__(self):
    return len(self._key_list)

  def keys(self): 
    return self._key_list[:]

  def values(self):
    return self._value_list[:]

  def items(self):
    return zip(self._key_list, self._value_list)
    
  def iterkeys(self):
    return self._key_list.__iter__()  

  def itervalues(self):
    return self._value_list.__iter__()

  def iteritems(self):
    def ii():
      for i in xrange(len(self._key_list)):
        yield self._key_list[i], self._value_list[i]
    return ii

  def has_key(self, key):
    return key in self

  def clear(self):
    self._key_list = simple_list()
    self._value_list = simple_list()

  def copy(self):
    cp = list_dict()
    cp._key_list = self._key_list[:]
    cp._value_list = self._value_list[:]
    return cp


