#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_forest_nodes.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Dec 24 08:31:53 2016 mstenber
# Last modified: Sat Dec 24 08:38:23 2016 mstenber
# Edit time:     6 min
#
"""

Minimal tests for sub-parts of that make sense to be tested alone.

"""

import collections

import forest_nodes


def test_block_id_referrer_without_forest():
    class DummyNode(forest_nodes.BlockIdReferrerMixin):
        forest = None
    dn = DummyNode()
    assert not dn.block_id
    dn.block_id = b'foo'
    assert dn.block_id == b'foo'
    dn.block_id = b'bar'
    assert dn.block_id == b'bar'


def test_block_id_referrer_with_forest():
    class DummyForest:
        block_id_references = collections.defaultdict(set)

        def nodes(self, key):
            return set(x.node for x in self.block_id_references[key])
    df = DummyForest()

    class DummyNode(forest_nodes.BlockIdReferrerMixin):
        forest = df

    dn = DummyNode()
    assert not dn.block_id
    dn.block_id = b'foo'
    assert dn.block_id == b'foo'
    dn.block_id = b'bar'
    assert dn.block_id == b'bar'

    dn2 = DummyNode()
    assert not dn2.block_id
    dn2.block_id = b'foo'
    assert dn2.block_id == b'foo'
    dn2.block_id = b'bar'
    assert dn2.block_id == b'bar'

    dn3 = DummyNode()
    assert not dn3.block_id
    dn3.block_id = b'bar'
    assert dn3.block_id == b'bar'
    dn3.block_id = b'foo'
    assert dn3.block_id == b'foo'

    assert df.nodes(b'foo') == set([dn3])
    assert df.nodes(b'bar') == set([dn, dn2])
