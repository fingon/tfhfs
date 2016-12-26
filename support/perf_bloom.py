#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: perf_bloom.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Mon Dec 26 09:20:11 2016 mstenber
# Last modified: Mon Dec 26 09:52:44 2016 mstenber
# Edit time:     6 min
#
"""Determine which is faster, big int or intarray bloom.

BigIntBloom insert        :      28.4256/sec [281ms] (35.18ms/call)
BigIntBloom insert+check  :      17.8798/sec [280ms] (55.929ms/call)
IntArrayBloom insert      :      30.3742/sec [263ms] (32.923ms/call)
IntArrayBloom insert+check:      17.3769/sec [288ms] (57.548ms/call)

By minor margin (given Python 3.6), intarray seems to take the cake
and is therefore the default.

"""

import os

TEST_DATA_ITEMS=10000
TEST_DATA_CHECKS=TEST_DATA_ITEMS

import random
import ms.perf

if __name__ == '__main__':
    global __package__
    if __package__ is None:
        import python3fuckup
        __package__ = python3fuckup.get_package(__file__, 1)
    import bloom
    hashes = [random.randint(0, 1<<30) for x in range(TEST_DATA_ITEMS)]
    def _hasher(i):
        yield hashes[i]

    def _insert(cl):
        o = cl(_hasher, n=TEST_DATA_ITEMS)
        for i in range(TEST_DATA_ITEMS):
            o.add(i)
        return o

    def _check(cl):
        o = _insert(cl)
        for i in range(TEST_DATA_CHECKS):
            assert o.has(i)

    l = []
    for cl in [bloom.BigIntBloom, bloom.IntArrayBloom]:
        def _insert1():
            _insert(cl)
        def _check1():
            _check(cl)
        l.append(('%s insert' % cl.__name__, _insert1))
        l.append(('%s insert+check' % cl.__name__, _check1))

    ms.perf.testList(l, maxtime=0.3)





