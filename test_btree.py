#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_btree.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Jun 25 16:29:53 2016 mstenber
# Last modified: Tue Dec 20 07:50:33 2016 mstenber
# Edit time:     46 min
#
"""

"""

import logging
import random

import pytest

import btree

_debug = logging.getLogger(__name__).debug


class NoHashLeafNode(btree.LeafNode):
    name_hash_size = 0


def test_simple():
    tn = btree.TreeNode()
    n1 = NoHashLeafNode(b'foo.txt')
    n2 = NoHashLeafNode(b'bar.txt')
    n3 = NoHashLeafNode(b'baz.txt')  # not added, used to test search variants
    tn = btree.TreeNode()

    # test addition/removal
    tn.add_child(n1)
    tn.add_child(n2)

    assert tn.search_prev_or_eq(n3) == n2
    assert tn.search(n3) == None
    assert tn.search(n2) == n2

    assert tn.children == [n2, n1]
    tn.remove_child(n2)
    assert tn.children == [n1]
    tn.remove_child(n1)
    assert tn.children == []

    # other way around..
    tn.add_child(n1)
    tn.add_child(n2)
    assert tn.children == [n2, n1]
    tn.remove_child(n1)
    assert tn.children == [n2]
    assert list(tn.get_leaves()) == [n2]


class LeafierTreeNode(btree.TreeNode):
    maximum_size = 2048
    minimum_size = maximum_size * 1 / 4
    has_spares_size = maximum_size / 2


@pytest.mark.run(order=-1)  # quite slow :p
@pytest.mark.parametrize('hashleaf', [False, True])
def test_large_tree(hashleaf):
    root = LeafierTreeNode()
    nodes = []
    cl = hashleaf and btree.LeafNode or NoHashLeafNode
    for i in range(1000):
        # all rebalancing options NOT covered: 100
        # all rebalancing options covered: 1000
        # (oh well, few seconds of CPU, who cares?)
        name = b'%04d' % i
        n = cl(name)
        n.i = i
        nodes.append(n)
    last_leaf_name = name
    random.shuffle(nodes)
    for i, n in enumerate(nodes):
        _debug('add #%d: %s', i, n)
        root = root.add(n)
        n2 = cl(n.name)
        # Ensure add result looks sane
        assert root.search(n2) is n
    assert len(list(root.get_leaves())) == len(nodes)

    print(root.depth, root.csize)
    # Then, with the fully formed tree, ensure nodes can still be found
    for i, n in enumerate(nodes):
        _debug('check #%d: %s', i, n)
        n2 = cl(n.name)
        assert root.search(n2) is n

    if not hashleaf:
        assert root.first_leaf.key == b'0000'
        assert root.last_leaf.key == last_leaf_name

    assert root.depth > 1

    assert root.root is root
    assert root.children[0].root is root

    # Randomly remove nodes from it; the tree should stay fully functional to
    # the bitter end.
    random.shuffle(nodes)
    for i, n in enumerate(nodes):
        _debug('remove #%d: %s', i, n)
        n2 = cl(n.name)
        # Ensure add result looks sane
        assert root.search(n2) is n
        root.remove(n)
        assert not root.search(n2)
    assert root.depth == 1
