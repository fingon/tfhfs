#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_inode.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Fri Nov 25 15:45:43 2016 mstenber
# Last modified: Sat Dec 24 06:45:52 2016 mstenber
# Edit time:     14 min
#
"""

"""

import pytest

from btree import LeafNode, TreeNode
from inode import INodeAllocator


def test():
    s = INodeAllocator(None, 12)
    root_n = TreeNode()
    root_inode = s.add_inode(node=root_n)
    assert s.get_by_node(root_n) is root_inode
    assert s.get_by_value(12) is root_inode
    assert root_inode.value == 12
    ln1 = LeafNode(b'foo')
    root_n.add_child(ln1)
    child_n = TreeNode()
    assert root_inode.refcnt == 1
    child_inode = s.add_inode(node=child_n, leaf_node=ln1)
    assert s.get_by_leaf_node(ln1) is child_inode
    assert root_inode.refcnt == 2
    assert child_inode.value == 13
    assert not s.remove_old_inodes()
    child_inode.deref()
    assert child_inode.refcnt == 0
    assert s.remove_old_inodes() == 1
    assert s.count() == 1

    # Test that 'None' inodes also work (=very leafy inodes)
    ln2 = LeafNode(b'bar')
    root_n.add_child(ln2)
    child_inode2 = s.add_inode(node=None, leaf_node=ln2)
    assert root_inode.refcnt == 2
    child_inode2.deref()
    assert s.remove_old_inodes() == 1
    assert s.count() == 1


def test_inodes_get_by_node_wrongtype():
    s = INodeAllocator(None, 13)
    with pytest.raises(AssertionError):
        s.get_by_node(None)


def test_inodes_get_by_node_nonexistent():
    s = INodeAllocator(None, 13)
    with pytest.raises(KeyError):
        s.get_by_node(TreeNode())
