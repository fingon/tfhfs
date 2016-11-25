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
# Created:       Tue Jul  5 11:49:58 2016 mstenber
# Last modified: Fri Nov 25 18:04:58 2016 mstenber
# Edit time:     51 min
#
"""

Test the 'forest' module

"""

import logging

import forest
from storage import NopBlockCodec, SQLiteStorage, TypedBlockCodec

_debug = logging.getLogger(__name__).debug


class LeafierDirectoryTreeNode(forest.DirectoryTreeNode):
    maximum_size = 2048
    minimum_size = maximum_size * 1 / 4
    has_spares_size = maximum_size / 2


def test_forest():
    storage = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    f = forest.Forest(storage, 42)
    root = f.get_inode_by_value(42)
    assert root
    root2 = f.root
    assert root is root2

    # add a 'file'
    file_inode = f.create_file(f.root, b'foo')
    file_parent = file_inode.parent_node
    assert f.flush()
    file_parent.set_data('foo', 42)
    assert f.flush()
    assert not f.flush()

    assert f.root.node.search_name(b'foo') == file_parent

    storage2 = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    storage2.conn = storage.conn
    f2 = forest.Forest(storage, 42)
    assert not f2.root.node.dirty
    assert f2.root.node.search(file_parent).data == dict(foo=42)
    assert not f2.root.node.dirty

    # add a directory
    dir_inode = f.create_dir(f.root, name=b'bar')
    subdir = dir_inode.parent_node
    assert f.root.node.dirty
    assert not dir_inode.node._block_id
    f.flush()
    assert dir_inode.node._block_id
    assert not f.flush()

    # Ensure that changing things _within the directory_ also makes
    # things happen.
    subdir.set_data('x', 43)
    assert subdir.dirty
    # assert f.root.dirty # n/a; the dirtiness is propagated during flush

    old_root_block_id = root.node._block_id
    assert old_root_block_id
    assert f.flush()
    assert root.node._block_id != old_root_block_id
    assert not f.flush()


def test_larger_forest():
    storage = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    f = forest.Forest(storage, 42)
    f.directory_node_class = LeafierDirectoryTreeNode
    for i in range(100):
        inode = f.create_dir(f.root, name=b'foo%d' % i)
    f.flush()

    storage2 = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    storage2.conn = storage.conn
    f2 = forest.Forest(storage, 7)
    assert f2.root.node.search(inode.parent_node).key == inode.parent_node.key
