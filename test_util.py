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
# Last modified: Mon Dec 19 16:18:53 2016 mstenber
# Edit time:     15 min
#
"""

"""

import util


def test_cborpickler():
    cbp = util.CBORPickler(dict(foo=42, bar=7))

    class Dummy:
        foo = 42

    o = Dummy()
    o.foo = 42
    assert cbp.get_external_dict(o) == {}

    o = Dummy()
    o.foo = None
    assert cbp.get_external_dict(o) == {42: None}

    o = Dummy()
    o.bar = None
    assert cbp.get_external_dict(o) == {}

    o = Dummy()
    o.foo = 'foov'
    assert cbp.get_external_dict(o) == {42: 'foov'}

    o2 = Dummy()
    cbp.load_external_dict_to(cbp.dumps(o), o2)
    assert vars(o) == vars(o2)
    cbp.unload_from(o2)
    assert vars(o) != vars(o2)
    assert vars(o2) == {}


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


def test_getrecsizeof_tuple():
    s1 = util.getrecsizeof(tuple([]))
    s2 = util.getrecsizeof(tuple([42]))
    s3 = util.getrecsizeof(tuple([42, 43]))
    assert s1 > 0
    assert s1 < s2
    assert s2 < s3


def test_to_bytes():
    assert util.to_bytes(b'foo') == b'foo'
    assert util.to_bytes('foo') == b'foo'
