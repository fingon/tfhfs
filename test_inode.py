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
# Last modified: Fri Nov 25 16:09:58 2016 mstenber
# Edit time:     7 min
#
"""

"""

import pytest

from btree import LeafNode, TreeNode
from inode import INodeStore


def test():
    s = INodeStore(13)
    root_n = TreeNode()
    root_inode = s.add_inode(node=root_n, value=7)
    assert s.get_inode_by_node(root_n) is root_inode
    assert s.get_inode_by_value(7) is root_inode
    assert root_inode.value == 7
    ln1 = LeafNode(b'foo')
    root_n.add_child(ln1)
    child_n = TreeNode()
    assert root_inode.refcnt == 1
    child_inode = s.add_inode(node=child_n, parent_node=ln1)
    assert s.get_inode_by_parent_node(ln1) is child_inode
    assert root_inode.refcnt == 2
    assert child_inode.value == 13
    assert not s.remove_old_inodes()
    child_inode.deref()
    assert child_inode.refcnt == 0
    assert s.remove_old_inodes() == 1
    assert s.count() == 1


@pytest.mark.xfail(raises=AssertionError)
def test_get_inode_by_node_wrongtype():
    s = INodeStore(13)
    s.get_inode_by_node(None)


@pytest.mark.xfail(raises=KeyError)
def test_get_inode_by_node_nonexistent():
    s = INodeStore(13)
    s.get_inode_by_node(TreeNode())
