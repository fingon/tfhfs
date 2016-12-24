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
# Last modified: Tue Dec 20 18:18:23 2016 mstenber
# Edit time:     97 min
#
"""

Test the 'forest' module

"""

import logging
import stat

import pytest

import forest
from storage import DictStorage

_debug = logging.getLogger(__name__).debug


class LeafierDirectoryTreeNode(forest.DirectoryTreeNode):
    maximum_size = 2048
    minimum_size = maximum_size * 1 / 4
    has_spares_size = maximum_size / 2


def testforest():
    storage = DictStorage()
    f = forest.Forest(storage, root_inode=42)
    root = f.inodes.get_by_value(42)
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

    f2 = forest.Forest(storage, root_inode=42)
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
    f = forest.Forest(storage)
    parent_inode = f.root
    test_depth = 10
    for i in range(test_depth):
        parent_inode = f.create_dir(parent_inode, b'dir')
        assert parent_inode
    parent_inode.leaf_node.set_data('foo', 42)
    f.flush()

    f2 = forest.Forest(storage)
    parent_inode = f2.root
    for i in range(test_depth):
        _debug('iteration #%d/%d', i + 1, test_depth)
        parent_inode = f2.lookup(parent_inode, b'dir')
    assert parent_inode.leaf_node.data['foo'] == 42


def test_wideforest():
    storage = DictStorage()
    f = forest.Forest(storage)
    f.directory_node_class = LeafierDirectoryTreeNode
    test_count = 100
    for i in range(test_count):
        inode = f.create_dir(f.root, name=b'foo%d' % i)
    f.flush()

    f2 = forest.Forest(storage)
    for i in range(test_count):
        inode = f2.lookup(f2.root, b'foo%d' % i)
        assert inode


@pytest.mark.run(order=0)
def test_merge3_file():
    remote_name = b'remote'
    remote_old_name = b'remote_old'

    _debug('# set up %s', remote_name)
    storage = DictStorage()
    rf = forest.Forest(storage, content_name=remote_name)
    same = rf.create_file(rf.root, name=b'foosame')
    rm = rf.create_file(rf.root, name=b'foorm')
    chg = rf.create_file(rf.root, name=b'foochange')
    rf.flush()

    _debug('# set up local')
    f = forest.Forest(storage)

    _debug('# attempt merge')
    f.merge_remote(remote_name, remote_old_name)
    for n in [same, rm, chg]:
        assert f.root.node.search_name(n.leaf_node.name)
    f.flush()

    # should be nop from here on onward (more or less)
    f.merge_remote(remote_name, remote_old_name)

    rf.create_file(rf.root, name=b'bar').deref()

    # change 'foo' (diff. data)
    rf.unlink(rf.root, name=chg.leaf_node.name)
    chg2 = rf.create_file(rf.root, name=chg.leaf_node.name)

    # TBD: remove file
    rf.unlink(rf.root, name=rm.leaf_node.name)
    rf.flush()

    f = forest.Forest(storage)
    _debug('# attempt merge changes')
    f.merge_remote(remote_name, remote_old_name)
    exp = [(rm, None), (same, same), (chg2, chg2)]
    for n, expn in exp:
        gotde = f.root.node.search_name(n.leaf_node.name)
        if not expn:
            assert not gotde
            continue
        assert expn.direntry.is_same(gotde)
