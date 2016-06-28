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
# Last modified: Tue Jun 28 13:38:58 2016 mstenber
# Edit time:     15 min
#
"""

"""

import forest
import random


def test_simple():
    tn = forest.TreeNode()
    n1 = forest.LeafNode(b'foo.txt')
    n2 = forest.LeafNode(b'bar.txt')
    tn = forest.TreeNode()

    # test addition/removal
    tn.add_child(n1)
    tn.add_child(n2)
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
    for i in range(10000):
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
    # Randomly remove nodes from it; the tree should stay fully functional to
    # the bitter end.
    random.shuffle(nodes)
    for n in nodes:
        n2 = forest.LeafNode(n.name)
        # Ensure add result looks sane
        assert root.search(n2) is n
        root.remove(n)
    assert root.depth == 1
