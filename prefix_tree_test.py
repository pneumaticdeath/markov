#!/usr/bin/python

# Copyright 2009, Mitch Patenaude

__author__ = 'Mitch Patenaude (patenaude@gmail.com)'

import unittest

import prefix_tree

class PrefixTreeTest(unittest.TestCase):
  def testMarkovChainInit(self):
    mc = prefix_tree.MarkovChain(max=3)
    self.assertEqual(0, mc.count, "Nonzero count in fresh instance")
    self.assertEqual([], mc._tuple_map.keys(),
                     "Non-empty tuple map in empty chain")
    self.assertTrue(mc._GetRandomKey() is None,
                    "Empty chain shouldn't return a tuple.")
    self.assertEqual(1, mc._max_key, "max key length not set properly")
    self.assertEqual(2, mc._min, "min depth not set properly")
    self.assertEqual(3, mc._max, "max tuple length not set properly")

  def testUpdateTuple(self):
    mc = prefix_tree.MarkovChain(max=4)
    mc._UpdateTuple(('a','b',))
    self.assertTrue(('a','b',) in mc._tuple_map, 
                    "Tuple didn't appear in tuple map.")
    self.assertEqual(1, mc._tuple_map[('a','b',)].count,
                     "tree count not incremented.")
    self.assertEqual(0, len(mc._tuple_map[('a','b')].keys()),
                     "Elements added to leaf incorrectly.")
    self.assertEqual(('a','b',), mc._GetRandomKey(), "Didn't get only extant tuple back")
    self.assertTrue(mc._tuple_map[('a','b',)].GetRandomTuple() == tuple(), "GetRandomTuple shouldn't find anything")
    self.assertEqual(1, mc.count, "Count incorrect after single update")

    mc._UpdateTuple(('x','y','z',))
    self.assertTrue(('x','y',) in mc._tuple_map,
                    "Update of non-trivial tuple missing key.")
    self.assertTrue('z' in mc._tuple_map[('x','y',)],
                    "last element in tuple missing from leaf.")
    self.assertEqual(('x','y','z',), mc.GetRandomTuple(('x','y',)),
                     "Next element not returned for updated tuple.")

  def testSequenceGeneration(self):
    mc = prefix_tree.MarkovChain(max=4)
    test_text = 'This has a long sequence of characters'
    mc.Update(test_text)
    result_text = ''.join(mc.GetRandomSequence(tuple(test_text[:3])))
    self.assertEqual(test_text, result_text, "Input was \"%s\" output was \"%s\"" % (test_text, result_text))

  def testWordSequenceGeneration(self):
    mc = prefix_tree.MarkovChain(max=3)
    test_text = 'This has a long sequence of words'
    mc.Update(test_text.split())
    result_text = ' '.join(mc.GetRandomSequence(tuple(test_text.split()[:2])))
    self.assertEqual(test_text, result_text, "Input was \"%s\" output was \"%s\"" % (test_text, result_text))

  def testChainUpdate(self):
    mc = prefix_tree.MarkovChain(max=4)
    mc.Update('abc')
    retSeq = mc._GetRandomKey(('a',))
    self.assertEqual(('a','b',), retSeq, "proper key not recovered with"
                     " appropriate seed: got %s" % repr(retSeq))
    fullSeq = ''.join(mc.GetRandomSequence(('a',)))
    self.assertEqual('abc', fullSeq, "Didn't full full sequence"
                     " back out, only %s" % fullSeq)

if __name__ == "__main__":
  unittest.main()   
