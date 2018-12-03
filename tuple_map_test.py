#!/usr/bin/python

# Copyright 2009, Mitch Patenaude

__author__ = 'Mitch Patenaude (patenaude@gmail.com)'

import unittest

from tuple_map import MarkovChain
from tuple_map import _MCLeaf

class LeafTest(unittest.TestCase):
  def testEndLeafCounters(self):
    """Test to make sure that counters are incremented properly """

    # null Creation
    end_leaf = _MCLeaf(None)
    self.assertEqual(1,end_leaf.count,
                     "Count of newly initiated leaf should be 1")
    self.assertFalse(end_leaf.elem.items(),"Should have no items")

    # null Update
    end_leaf.Update(None)
    self.assertEqual(2,end_leaf.count,
                     "Count not incremented by Update()")
    self.assertFalse(end_leaf.elem.items(), "Should have no items")

    # non null initial update
    end_leaf.Update('a')
    self.assertTrue('a' in end_leaf.elem, "Updated element should exist")
    self.assertEqual(end_leaf.elem['a'], 1, "Count of update element wrong.")
    self.assertEqual(['a',],end_leaf.elem.keys(), "Contents incorrect: %s"
                     % repr(end_leaf.elem.keys()))
    self.assertEqual(3,end_leaf.count,"leaf counter not incremented")
    
  def testLeafCounters(self):
    """Test leaf counters for non null values"""
    leaf = _MCLeaf('a')
    
    # instansiation tests
    self.assertEqual(1, leaf.count, "leaf counter not initialized correctly")
    self.assertTrue('a' in leaf.elem, "element not added")
    self.assertEqual(1, leaf.elem['a'], "element count not initialized correctly")
    self.assertEqual(1, len(leaf.elem.keys()), "too many elements in leaf")

    # update of existing element
    leaf.Update('a')
    self.assertTrue('a' in leaf.elem, "element disappeared after update")
    self.assertEqual(2,leaf.elem['a'],"element counter not updated")
    self.assertEqual(2,leaf.count, "leaf count not updated")
    
    # update of new element
    self.assertFalse('b' in leaf.elem, "Where did 'b' come from?")
    leaf.Update('b')
    self.assertTrue('b' in leaf.elem, "new element not added.")
    self.assertEqual(1,leaf.elem['b'], "Update() element count not initialized")
    self.assertEqual(3,leaf.count, "Update leaf count not incremented")
    self.assertTrue('a' in leaf.elem, "Where did 'a' go?")
    self.assertEqual(2,len(leaf.elem.keys()), "Unexpected elements appeared.")

  def testLabelUpdates(self):
    leaf = _MCLeaf(None)
    self.assertEqual(set(), leaf.labels,
                     "Label set not created properly for no label")
    self.assertEqual({}, leaf.elem_labels,
                     "Element label set not created properly")
    
    leaf.Update(None, label='terminating')
    self.assertEqual(set(['terminating',]), leaf.labels,
                     "Label set not updated properly for terminating label.")
    self.assertFalse(leaf.elem_labels,
                     "element labels shouldn't up updated by terminating label")
    
    leaf.Update('a', label='label_a')
    self.assertEqual(set(['terminating',]), leaf.labels,
                     "terminating labels shouldn't be updated by non-terminating update.")
    self.assertTrue('a' in leaf.elem_labels, 
                    "element labels not created")
    self.assertEqual(set(['label_a',]), leaf.elem_labels['a'],
                     "element labels not applied")

class MarkovTest(unittest.TestCase):
  def testMarkovChainInit(self):
    mc = MarkovChain(max=3)
    self.assertEqual(0, mc.count, "Nonzero count in fresh instance")
    self.assertEqual([], mc._tuple_map.keys(),
                     "Non-empty tuple map in empty chain")
    # self.assertTrue(mc._GetRandomKey() is None,
    #                 "Empty chain shouldn't return a tuple.")
    self.assertEqual(2, mc._max_key, "max key length not set properly")
    self.assertEqual(3, mc._max, "max tuple length not set properly")

  def testUpdateTuple(self):
    mc = MarkovChain(max=3)
    mc._UpdateTuple(('a','b',))
    used = set()
    self.assertTrue(('a','b',) in mc._tuple_map, 
                    "Tuple didn't appear in tuple map.")
    self.assertEqual(1, mc._tuple_map[('a','b',)].count,
                     "Leaf count not incremented.")
    self.assertEqual(0, len(mc._tuple_map[('a','b')].elem.keys()),
                     "Elements added to leaf incorrectly.")
    self.assertEqual(('a','b',), mc._GetRandomKey(),
                     "Didn't get only extant tuple back")
    self.assertTrue(mc._GetNext(('a','b',), labelset=used) is None,
                    "_GetNext should indicate termination")
    self.assertFalse(used, "No labels should be return for this tuple")
    self.assertEqual(1, mc.count, "Count incorrect after single update")

    used.clear()
    mc._UpdateTuple(('x','y','z',), label='label_xyz')
    self.assertTrue(('x','y',) in mc._tuple_map,
                    "Update of non-trivial tuple missing key.")
    self.assertTrue('z' in mc._tuple_map[('x','y',)].elem,
                    "last element in tuple missing from leaf.")
    self.assertEqual('z', mc._GetNext(('x','y',), labelset=used),
                     "Next element not returned for updated tuple.")
    self.assertTrue('label_xyz' in used, "Label from update not returned")

  def testSequenceGeneration(self):
    mc = MarkovChain(max=4)
    test_text = 'This has a long sequence of characters'
    mc.Update(test_text)
    result_text = ''.join(mc.GetRandomSequence(tuple(test_text[:3])))
    self.assertEqual(test_text, result_text, "Input was \"%s\" output was \"%s\"" % (test_text, result_text))

  def testWordSequenceGeneration(self):
    mc = MarkovChain(max=3)
    test_text = 'This has a long sequence of words'
    mc.Update(test_text.split())
    result_text = ' '.join(mc.GetRandomSequence(tuple(test_text.split()[:2])))
    self.assertEqual(test_text, result_text, "Input was \"%s\" output was \"%s\"" % (test_text, result_text))

  def testChainUpdate(self):
    mc = MarkovChain(max=3)
    mc.Update('abc')
    retSeq = mc._GetRandomKey(('a',))
    self.assertEqual(('a','b',), retSeq, "Initial Tuple not recovered with"
                     " appropriate seed: got %s" % repr(retSeq))
    fullSeq = ''.join(mc.GetRandomSequence(('a',)))
    self.assertEqual('abc', fullSeq, "Didn't full full sequence"
                     " back out, only %s" % fullSeq)

if __name__ == "__main__":
  unittest.main()   
