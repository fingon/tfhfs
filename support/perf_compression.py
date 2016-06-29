#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: perf_compression.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Wed Jun 29 11:05:26 2016 mstenber
# Last modified: Wed Jun 29 11:19:26 2016 mstenber
# Edit time:     11 min
#
"""lz4 t 10      :  131942.8817/sec [98ms] (7.579us/call)
lz4 t 100k    :   56069.1720/sec [99.4ms] (17.835us/call)
lz4 t 100k (d):   18504.4483/sec [97.5ms] (54.041us/call)
lz4 g 100k HC :     536.3362/sec [97ms] (1.8645ms/call)
lz4 g 100k    :   79519.7382/sec [98.7ms] (12.575us/call)
lz4 g 100k (d):  136562.9426/sec [97.3ms] (7.3226us/call)

Interestingly enough, the worst performance (just 1.8 GB/s) was
decompressing highly compressed data..  Still, this is essentially
free.

'HC' compression is not, though, it is 100x as expensive and even order of magnitude more expensive than SHA256.

"""

import lz4  # pip install lz4
import ms.perf
import os

text10 = b'1234567890'
text100k = text10 * 10000
assert len(text100k) == 100000
garbage100k = os.urandom(len(text100k))


def test_lz4_1():
    lz4.compressHC(text10)


def test_lz4_2():
    lz4.compressHC(text100k)

compressed100k = lz4.compressHC(text100k)

print('text100k compressed length', len(compressed100k))


def test_lz4_3():
    s = lz4.loads(compressed100k)
    #assert len(s) == len(text100k)


def test_lz4_4():
    lz4.compressHC(garbage100k)


def test_lz4_4_2():
    lz4.compress(garbage100k)


compressedgarbage100k = lz4.compressHC(garbage100k)


def test_lz4_5():
    s = lz4.loads(compressedgarbage100k)
    #assert len(s) == len(text100k)

print('garbage100k compressed length', len(compressedgarbage100k))

ms.perf.testList([['lz4 t 10', test_lz4_1],
                  ['lz4 t 100k', test_lz4_2],
                  ['lz4 t 100k (d)', test_lz4_3],
                  ['lz4 g 100k HC', test_lz4_4],
                  ['lz4 g 100k', test_lz4_4_2],
                  ['lz4 g 100k (d)', test_lz4_5]],
                 maxtime=0.1)
