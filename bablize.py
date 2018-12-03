#!/usr/bin/python

# Copyright 2009, Mitch Patenaude

from markov.tuple_map import MarkovChain
import sys

def bablize(infile,outfile,depth=5):
  mkv = MarkovChain(max=depth)
  para = []
  firsts = []
  for line in infile:
    if line.isspace():
      if para:
        firsts.append(tuple(para[0:mkv._max-1]))
        mkv.Update(para)
        para = []
    else:
      para += line.split()

  if para:
    firsts.append(tuple(para[0:mkv._max-1]))
    mkv.Update(para)

  # we shouldn't close a file that we didn't open.
  # infile.close()

  for seed in firsts:
    outfile.write(" ".join(mkv.GetRandomSequence(seed=seed))+"\n\n")
  # we shouldn't close a file that we didn't open.
  # outfile.close()

if __name__ == '__main__':
  if sys.argv and sys.argv[0].isdigit():
    depth=int(sys.argv[0])
  else:
    depth=5

  bablize(sys.stdin, sys.stdout, depth)

  
