#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_bloom.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Mon Dec 26 09:36:20 2016 mstenber
# Last modified: Mon Dec 26 09:50:48 2016 mstenber
# Edit time:     6 min
#
"""

"""

from bloom import IntArrayBloom, BigIntBloom
import pytest


@pytest.fixture(params=['array', 'bigint'])
def bloom_class(request):
    return {'array': IntArrayBloom, 'bigint': BigIntBloom}[request.param]


def _nophasher(v):
    yield v


def test_bloom(bloom_class):
    b = bloom_class(_nophasher, n=10)
    assert b.count == 0
    assert not b.has(1)
    b.add(1)
    assert b.has(1)
    assert not b.has(2)
    b.add(2)
    assert b.has(2)
    assert b.grow() is b
    assert b.count > 0
    assert b.count < 10

    for i in range(8):
        b.add(i)

    nb = b.grow()
    assert nb is not b
    assert nb.has(7)
    assert not nb.has(8)


def test_bloom_huge(bloom_class):
    b = bloom_class(_nophasher, n=10)
    for i in range(20):
        b.add(i)
    assert b.count == float('inf')
