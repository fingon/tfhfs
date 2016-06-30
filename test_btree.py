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
# Last modified: Thu Jun 30 14:24:27 2016 mstenber
# Edit time:     28 min
#
"""

"""

import btree
import random
import pytest


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


class LeafierTreeNode(btree.TreeNode):
    maximum_size = 2048
    minimum_size = maximum_size * 1 / 4
    has_spares_size = maximum_size / 2


def test_large_tree():
    root = LeafierTreeNode()
    nodes = []
    for i in range(1000):
        name = b'%04d' % i
        n = btree.LeafNode(name)
        n.i = i
        nodes.append(n)
    random.shuffle(nodes)
    for n in nodes:
        root = root.add(n)
        n2 = btree.LeafNode(n.name)
        # Ensure add result looks sane
        assert root.search(n2) is n

    print(root.depth, root.csize)
    # Then, with the fully formed tree, ensure nodes can still be found
    for n in nodes:
        n2 = btree.LeafNode(n.name)
        assert root.search(n2) is n

    assert root.depth > 1

    assert root.root == root
    assert root.children[0].root == root

    # Randomly remove nodes from it; the tree should stay fully functional to
    # the bitter end.
    random.shuffle(nodes)
    for n in nodes:
        n2 = btree.LeafNode(n.name)
        # Ensure add result looks sane
        assert root.search(n2) is n
        root.remove(n)
    assert root.depth == 1


@pytest.mark.xfail(raises=TypeError)
def test_eq_1():
    btree.LeafNode(b'x') < 1


def test_eq_2():
    btree.LeafNode(b'x') == 1
