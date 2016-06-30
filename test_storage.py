#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_storage.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Wed Jun 29 10:36:03 2016 mstenber
# Last modified: Thu Jun 30 12:59:07 2016 mstenber
# Edit time:     16 min
#
"""

"""

from storage import SQLiteStorage, DelayedStorage
import pytest
import sqlite3


def _nop():
    pass


def _prod_storage(s, flush=_nop):
    # refcnt = 1
    s.store_block(b'foo', b'bar')
    flush()
    assert s.get_block_data_by_id(b'foo') == b'bar'

    # refcnt = 2
    s.store_block(b'foo')
    flush()
    assert s.get_block_data_by_id(b'foo') == b'bar'

    # refcnt = 1
    assert s.release_block(b'foo')
    flush()
    assert s.get_block_data_by_id(b'foo') == b'bar'

    # refcnt = 0 => should be gone
    assert not s.release_block(b'foo')
    flush()
    assert s.get_block_data_by_id(b'foo') == None

    assert s.get_block_id_by_name(b'foo') == None
    s.set_block_id_name(b'bar', b'foo')
    flush()
    assert s.get_block_id_by_name(b'foo') == b'bar'

    s.set_block_id_name(None, b'foo')
    flush()
    assert s.get_block_id_by_name(b'foo') == None

    # Assume it did not have dependency handling before; add it now:
    deps = {b'content1': [b'id2']}

    def _depfun(block_id):
        return deps.get(block_id, [])
    s.block_data_references_callback = _depfun
    s.store_block(b'id2', b'content2')
    s.store_block(b'id1', b'content1')
    flush()
    assert s.get_block_by_id(b'id2')[1] == 2
    assert s.get_block_by_id(b'id1')[1] == 1
    s.release_block(b'id1')
    flush()
    assert s.get_block_by_id(b'id2')[1] == 1
    s.release_block(b'id2')
    flush()


def test_sqlitestorage():
    s = SQLiteStorage()
    _prod_storage(s)


@pytest.mark.xfail(raises=sqlite3.OperationalError)
def test_sqlitestorage_error():
    s = SQLiteStorage()
    s._get_execute_result('BLORB')


def test_sqlitestorage_error_ignored():
    s = SQLiteStorage()
    s._get_execute_result('BLORB', ignore_errors=True)


def test_delayedstorage_immediate_flush():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    _prod_storage(s, s.flush)


def test_delayedstorage_immediate_flush_no_cache():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    s.maximum_cache_size = 1
    _prod_storage(s, s.flush)


def test_delayedstorage_immediate_flush_no_dirty():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    s.maximum_dirty_size = 1
    _prod_storage(s, s.flush)


def test_delayedstorage_no_dirty():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    s.maximum_dirty_size = 1
    _prod_storage(s)


def test_delayedstorage():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    _prod_storage(s)

    # Should be nop as we deleted everything we added
    assert not s.flush()
    assert not s.cache_size
    assert not s.dirty_size

    # refcnt = 1
    s.store_block(b'foo', b'bar')
    assert s.get_block_data_by_id(b'foo') == b'bar'
    assert not s2.get_block_data_by_id(b'foo') == b'bar'
    assert not s.cache_size
    assert s.dirty_size

    assert s.flush() == 1
    assert s2.get_block_data_by_id(b'foo') == b'bar'
    assert s.cache_size
    assert not s.dirty_size

    assert s._blocks
    s.flush()
    assert s._blocks

    s.maximum_cache_size = 1
    s.flush()
    assert not s._blocks
