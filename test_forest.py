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
# Last modified: Sat Nov 19 10:43:02 2016 mstenber
# Edit time:     30 min
#
"""

Test the 'forest' module

"""

import forest
from storage import NopBlockCodec, SQLiteStorage, TypedBlockCodec


class LeafierDirectoryTreeNode(forest.DirectoryTreeNode):
    maximum_size = 2048
    minimum_size = maximum_size * 1 / 4
    has_spares_size = maximum_size / 2


def test_forest():
    storage = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    f = forest.Forest(storage, 42)
    assert not f.get_inode(7)
    root = f.get_inode(42)
    assert root
    root2 = f.root
    assert root is root2
    root.set_data('test', 42)
    assert root.data['test'] == 42
    assert f.flush()
    assert not f.flush()

    storage2 = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    storage2.conn = storage.conn
    f2 = forest.Forest(storage, 42)
    assert not f2.root.dirty
    assert f2.root.data['test'] == 42

    # add a 'file'
    de1 = forest.DirectoryEntry(f)
    de1.name = b'foo'
    f.add_child(f.root, de1)
    assert f.flush()
    assert not f.flush()

    storage2 = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    storage2.conn = storage.conn
    f2 = forest.Forest(storage, 42)
    assert not f2.root.dirty
    assert f2.root.search(de1).key == de1.key
    assert not f2.root.dirty

    # add a directory
    inode_subdir, subdir = f.create_dir_inode()
    de2 = forest.DirectoryEntry(f)
    de2.name = b'bar'

    assert not f.root.dirty
    f.add_child(f.root, de2)
    assert f.root.dirty
    de2.set_inode(inode_subdir)
    assert de2 in f.inode2deps.get(inode_subdir, [])
    de2.set_inode(inode_subdir)
    assert de2 in f.inode2deps.get(inode_subdir, [])
    de2.set_inode(None)
    assert not de2 in f.inode2deps.get(inode_subdir, [])
    de2.set_inode(inode_subdir)
    assert de2 in f.inode2deps.get(inode_subdir, [])
    assert de2.dirty
    assert not de2._block_id
    f.flush()
    assert not de2.dirty
    assert de2._block_id

    assert not f.flush()

    # Ensure that changing things _within the directory_ also makes
    # things happen.
    subdir.set_data('x', 43)
    assert subdir.dirty
    # assert f.root.dirty # n/a; the dirtiness is propagated during flush

    old_root_block_id = root._block_id
    assert old_root_block_id
    assert f.flush()
    assert root._block_id != old_root_block_id
    assert not f.flush()


def test_larger_forest():
    storage = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    f = forest.Forest(storage, 42)
    f.directory_node_class = LeafierDirectoryTreeNode
    for i in range(100):
        de = forest.DirectoryEntry(f)
        de.name = b'foo%d' % i
        f.add_child(f.root, de)
    f.flush()

    storage2 = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    storage2.conn = storage.conn
    f2 = forest.Forest(storage, 7)
    assert f2.root.search(de).key == de.key
