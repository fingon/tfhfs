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
# Last modified: Tue Jul  5 13:13:04 2016 mstenber
# Edit time:     9 min
#
"""

Test the 'forest' module

"""

import forest
from storage import NopBlockCodec, SQLiteStorage, TypedBlockCodec


def test_forest():
    storage = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    f = forest.Forest(storage, 42)
    assert not f.get_dir_inode(7)
    root = f.get_dir_inode(42)
    assert root
    root2 = f.root
    assert root is root2
    root.set('test', 42)
    assert root.data['test'] == 42
    assert f.flush()
    assert not f.flush()

    storage2 = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    storage2.conn = storage.conn
    f2 = forest.Forest(storage, 42)
    assert not f2.root.dirty
    assert f2.root.data['test'] == 42

    de1 = forest.DirectoryEntry(f)
    de1.name = b'foo'
    f.root.add(de1)
    assert f.flush()
    assert not f.flush()

    storage2 = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
    storage2.conn = storage.conn
    f2 = forest.Forest(storage, 42)
    assert not f2.root.dirty
    assert f2.root.search(de1).key == de1.key
    assert not f2.root.dirty
