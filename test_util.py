#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_util.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Fri Dec 16 06:47:30 2016 mstenber
# Last modified: Fri Dec 16 06:51:34 2016 mstenber
# Edit time:     4 min
#
"""

"""

import util


def test_getrecsizeof_dict():
    d = {}
    d2 = {}
    d2['42'] = d2  # nested loop
    s1 = util.getrecsizeof(d)
    s2 = util.getrecsizeof(d2)
    assert s1 > 0
    assert s1 < s2
    d3 = d2.copy()
    d3[7] = 1
    s3 = util.getrecsizeof(d3)
    assert s2 < s3


def test_getrecsizeof_list():
    s1 = util.getrecsizeof([])
    s2 = util.getrecsizeof([42])
    s3 = util.getrecsizeof([42, 43])
    assert s1 > 0
    assert s1 < s2
    assert s2 < s3


def test_to_bytes():
    assert util.to_bytes(b'foo') == b'foo'
    assert util.to_bytes('foo') == b'foo'
