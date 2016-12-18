#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: testforest.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Tue Jul  5 11:49:58 2016 mstenber
# Last modified: Sun Dec 18 20:06:11 2016 mstenber
# Edit time:     74 min
#
"""

Test the 'forest' module

"""

import logging
import stat

import forest
from storage import DictStorage

_debug = logging.getLogger(__name__).debug


class LeafierDirectoryTreeNode(forest.DirectoryTreeNode):
    maximum_size = 2048
    minimum_size = maximum_size * 1 / 4
    has_spares_size = maximum_size / 2


def testforest():
    storage = DictStorage()
    f = forest.Forest(storage, 42)
    root = f.get_inode_by_value(42)
    assert root
    root2 = f.root
    assert root is root2

    # add a 'file'
    file_inode = f.create_file(f.root, b'foo')
    file_parent = file_inode.leaf_node
    assert f.flush()
    file_parent.set_data('foo', 42)
    assert f.flush()
    assert not f.flush()

    assert f.root.node.search_name(b'foo') == file_parent

    f2 = forest.Forest(storage, 42)
    assert not f2.root.node.dirty
    assert f2.lookup(f2.root, b'nonexistent') is None
    f2c = f2.lookup(f2.root, b'foo')
    f2c2 = f2.lookup(f2.root, b'foo')
    assert f2c is f2c2
    assert f2c
    d = f2c.leaf_node.nonempty_data
    del d['st_ctime_ns']
    del d['st_mtime_ns']
    assert d == dict(foo=42, st_mode=stat.S_IFREG)
    assert not f2.root.node.dirty

    # add a directory
    dir_inode = f.create_dir(f.root, name=b'bar')
    subdir = dir_inode.leaf_node
    assert f.root.node.dirty
    assert not dir_inode.node.block_id
    f.flush()
    assert dir_inode.node.block_id
    assert not f.flush()

    # Ensure that changing things _within the directory_ also makes
    # things happen.
    subdir.set_data('foo', 43)
    assert subdir.dirty
    # assert f.root.dirty # n/a; the dirtiness is propagated during flush

    old_root_block_id = root.node.block_id
    assert old_root_block_id
    assert f.flush()
    assert root.node.block_id != old_root_block_id
    assert not f.flush()


def test_deepforest():
    storage = DictStorage()
    f = forest.Forest(storage, 42)
    parent_inode = f.root
    test_depth = 10
    for i in range(test_depth):
        parent_inode = f.create_dir(parent_inode, b'dir')
        assert parent_inode
    parent_inode.leaf_node.set_data('foo', 42)
    f.flush()

    f2 = forest.Forest(storage, 42)
    parent_inode = f2.root
    for i in range(test_depth):
        _debug('iteration #%d/%d', i + 1, test_depth)
        parent_inode = f2.lookup(parent_inode, b'dir')
    assert parent_inode.leaf_node.data['foo'] == 42


def test_wideforest():
    storage = DictStorage()
    f = forest.Forest(storage, 42)
    f.directory_node_class = LeafierDirectoryTreeNode
    test_count = 100
    for i in range(test_count):
        inode = f.create_dir(f.root, name=b'foo%d' % i)
    f.flush()

    f2 = forest.Forest(storage, 7)
    for i in range(test_count):
        inode = f2.lookup(f2.root, b'foo%d' % i)
        assert inode
