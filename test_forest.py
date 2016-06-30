#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_forest.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Jun 25 16:29:53 2016 mstenber
# Last modified: Thu Jun 30 13:06:10 2016 mstenber
# Edit time:     27 min
#
"""

"""

import forest
import random
import pytest


def test_simple():
    tn = forest.TreeNode()
    n1 = forest.LeafNode(b'foo.txt')
    n2 = forest.LeafNode(b'bar.txt')
    n3 = forest.LeafNode(b'baz.txt')  # not added, used to test search variants
    tn = forest.TreeNode()

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


class LeafierTreeNode(forest.TreeNode):
    maximum_size = 2048
    minimum_size = maximum_size * 1 / 4
    has_spares_size = maximum_size / 2


def test_large_tree():
    root = LeafierTreeNode()
    nodes = []
    for i in range(1000):
        name = b'%04d' % i
        n = forest.LeafNode(name)
        n.i = i
        nodes.append(n)
    random.shuffle(nodes)
    for n in nodes:
        root = root.add(n)
        n2 = forest.LeafNode(n.name)
        # Ensure add result looks sane
        assert root.search(n2) is n

    print(root.depth, root.csize)
    # Then, with the fully formed tree, ensure nodes can still be found
    for n in nodes:
        n2 = forest.LeafNode(n.name)
        assert root.search(n2) is n

    assert root.depth > 1

    assert root.root == root
    assert root.children[0].root == root

    # Randomly remove nodes from it; the tree should stay fully functional to
    # the bitter end.
    random.shuffle(nodes)
    for n in nodes:
        n2 = forest.LeafNode(n.name)
        # Ensure add result looks sane
        assert root.search(n2) is n
        root.remove(n)
    assert root.depth == 1


@pytest.mark.xfail(raises=TypeError)
def test_eq_1():
    forest.LeafNode(b'x') < 1


def test_eq_2():
    forest.LeafNode(b'x') == 1
