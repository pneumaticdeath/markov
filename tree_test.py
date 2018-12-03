#!/usr/bin/python

# Copyright 2009, Mitch Patenaude

__author__ = 'Mitch Patenaude (patenaude@gmail.com)'

import unittest

from tree import MarkovChain
from tree import _MCLeaf

class LeafTest(unittest.TestCase):
  def testEndLeafCounters(self):
    """Test to make sure that counters are incremented properly """

    # null Creation
    end_leaf = _MCLeaf()
    self.assertEqual(0,end_leaf.count,
                     "Count of newly initiated leaf should be 1")
    self.assertEqual(set(), end_leaf.labels, 
                     "Labels are not empty on creation")

    # null Update
    end_leaf._UpdateTuple(tuple())
    self.assertEqual(1,end_leaf.count,
                     "Count not incremented by Update()")
    self.assertEqual(set(), end_leaf.labels,
                     "Labels are not empty after non-label update")

    end_leaf._UpdateTuple(tuple(), label = 'a')
    self.assertEqual(2, end_leaf.count,
                     "Count not incremented by Update() with label")
    self.assertTrue('a' in end_leaf.labels, 
                    "Label not present in label set after update")

class MarkovTest(unittest.TestCase):
  def testMarkovChainInit(self):
    mc = MarkovChain(max=3)
    self.assertEqual(0, mc.count, "Nonzero count in fresh instance")
    self.assertFalse(mc.keys(),
                     "Non-empty element map in empty chain")
    self.assertEqual(tuple(), mc.GetRandomTuple(),
                    "Empty chain should return an empty tuple.")
    self.assertEqual(3, mc._max, "max tuple length not set properly")
    self.assertEqual(set(), mc.labels, "Labelset is not a null set")

  def testUpdateTuple(self):
    mc = MarkovChain(max=3)
    mc._UpdateTuple(('a','b',), label='first')
    self.assertTrue('a' in mc, 
                    "1st element in tuple didn't appear in tuple map.")
    self.assertEqual(1, mc.count, "Root counter not implemented properly")
    self.assertTrue('b' in mc['a'], 
                    "2nd element in tuple didn't appear in tuple map.")
    self.assertEqual(1, mc['a'].count, 
                     "2nd level counter not implemented properly,")
    self.assertFalse(mc['a']['b'].keys(),
                     "No 3rd level should have been created.")

    self.assertEqual(set(['first',]), mc['a']['b'].labels,
                     "Label not attached to tuple, what's there is %s"
                        % (mc['a']['b'].labels,))

    used_labels = set()
    self.assertEqual(('a','b',), mc.GetRandomTuple(None, labelset=used_labels),
                     "Didn't get only extant tuple back")
    self.assertEqual(set(['first',]), used_labels,
                     "Set of used labels wasn't correct, got %s"
                      % (used_labels,))

    mc._UpdateTuple(('x','y','z',), label='second')
    self.assertTrue('z' in mc['x']['y'],
                    "Update of non-trivial tuple missing key.")
    self.assertTrue(isinstance(mc['x']['y']['z'], _MCLeaf), 
                    "Leaf instance should follow max depth tuple.")
    self.assertTrue('second' in mc['x']['y']['z'].labels,
                    "Label missing for leaf node. present: %s" 
                    % (mc['x']['y']['z'].labels,))

  def testChainUpdate(self):
    mc = MarkovChain(max=3)
    mc.Update('abc')
    retSeq = mc.GetRandomTuple(('a',))
    self.assertEqual(('a','b','c',), retSeq, "Initial Tuple not recovered with"
                     " appropriate seed: got %s" % repr(retSeq))
    fullSeq = ''.join(mc.GetRandomSequence(('a',)))
    self.assertEqual('abc', fullSeq, "Didn't full full sequence"
                     " back out, only %s" % fullSeq)

  def testMinDepthUpdates(self):
    mc = MarkovChain(max=4,min=2)
    mc.Update('abcde')
    self.assertTrue('a' in mc,
                     "first element should be at the head of a chain")
    self.assertTrue('b' in mc,
                    "second element should be at head of a chain")
    self.assertTrue('c' in mc,
                    "third element should be at the head of a chain")
    self.assertTrue('d' in mc,
                     "fourth element should be at the head of a chain")
    self.assertFalse('e' in mc,
                     "Last element should not be at the head of a chain")
    self.assertTrue('e' in mc['d'], "Final two should be in the chain")
    self.assertEqual(1, mc['d']['e'].count,
                     "Count of this sequence should be 1.")
    self.assertEqual(0, len(mc['d']['e']),
                     "the final two should have no additional elements")

  def testMinDepthAutoSet(self):
    mc = MarkovChain(max=6)
    self.assertEqual(5, mc._min, "Min value should have a default of max-1")

if __name__ == "__main__":
  unittest.main()   
