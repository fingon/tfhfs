#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: bloom.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Mon Dec 26 08:59:37 2016 mstenber
# Last modified: Mon Dec 26 09:52:58 2016 mstenber
# Edit time:     34 min
#
"""

This is a bloom filter implementation that does not require fixed
maximum size as 'grow' method can be used to grow it (at cost of
doubling lookup speed). Still, lookups remain constant cost and
hopefully relatively cheap.

"""

import math
import sys

DEFAULT_N_ESTIMATE = 1000000
GROWTH_FACTOR = 100


class AbstractBloom:

    def __init__(self, hasher, *, k=1, n=DEFAULT_N_ESTIMATE, old=None):
        assert k > 0
        assert n > 0
        self.n = n
        self.m = int(k * n / math.log(2))
        self.k = k
        self.hasher = hasher
        self.v = 0
        self.old = old
        self.set_bits = 0

    def add(self, o):
        hash_iterator = self.hasher(o)
        for i in range(self.k):
            self.set_bit(next(hash_iterator))

    def add_bit(self, v):
        raise NotImplementedError

    @property
    def count(self):
        """ Estimate number of items (n) in the set """
        if self.set_bits == self.m:
            return float('Inf')
        return -self.m / self.k * math.log(1 - self.set_bits / self.m)

    def grow(self):
        if self.count > self.n:
            return self.__class__(self.hasher, k=self.k,
                                  n=self.n * GROWTH_FACTOR, old=self)
        return self

    def has_bit(self, v):
        raise NotImplementedError

    def has(self, o):
        hash_iterator = self.hasher(o)
        for i in range(self.k):
            v = next(hash_iterator)
            v = v % self.m
            if self.has_bit(v):
                return True
        if self.old is not None:
            return self.old.has(o)

    def set_bit(self, v):
        v = v % self.m
        if self.has_bit(v):
            return
        self.set_bits += 1
        self.add_bit(v)
        return self


class BigIntBloom(AbstractBloom):

    def __init__(self, *a, **kw):
        AbstractBloom.__init__(self, *a, **kw)
        self.value = 0

    def add_bit(self, v):
        self.value = self.value | (1 << v)

    def has_bit(self, v):
        return self.value & (1 << v)


class IntArrayBloom(AbstractBloom):

    def __init__(self, *a, **kw):
        AbstractBloom.__init__(self, *a, **kw)
        self.bits_per_int = sys.maxsize.bit_length()
        # ^ could be class value, but I think instance values are
        # slightly faster
        self.value = [0] * int((self.m // self.bits_per_int) + 1)

    def add_bit(self, v):
        bpi = self.bits_per_int
        idx = int(v // bpi)
        ofs = v % bpi
        self.value[idx] |= 1 << ofs

    def has_bit(self, v):
        bpi = self.bits_per_int
        idx = int(v // bpi)
        ofs = v % bpi
        return self.value[idx] & (1 << ofs)

Bloom = IntArrayBloom  # marginally faster, it seems
