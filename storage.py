#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: storage.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Wed Jun 29 10:13:22 2016 mstenber
# Last modified: Thu Jun 30 13:29:48 2016 mstenber
# Edit time:     98 min
#
"""

This is the 'storage layer' main module.

It is provides abstract interface for the forest layer to use, and
uses storage backend for actual raw file operations.

"""

import sqlite3
import time
import logging

_debug = logging.getLogger(__name__).debug


class Storage:

    def __init__(self, block_data_references_callback=None):
        self.block_data_references_callback = block_data_references_callback

    def get_block_data_references(self, block_data):
        if not self.block_data_references_callback:
            return
        yield from self.block_data_references_callback(block_data)

    def get_block_by_id(self, block_id):
        """Get data for the block identified by block_id. If the block does
not exist, None is returned. If it exists, (block data, reference count) tuple is returned."""
        raise NotImplementedError

    def get_block_data_by_id(self, block_id):
        r = self.get_block_by_id(block_id)
        if r:
            return r[0]

    def get_block_id_by_name(self, n):
        """Get the block identifier for the named block. If the name is not
set, None is returned."""
        raise NotImplementedError

    def on_add_block_data(self, block_data):
        for block_id in self.get_block_data_references(block_data):
            self.refer_block(block_id)

    def on_delete_block_id(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r
        (block_data, block_refcnt) = r
        for block_id2 in self.get_block_data_references(block_data):
            self.release_block(block_id2)

    def refer_block(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r
        self.set_block_refcnt(block_id, r[1] + 1)

    def release_block(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r and r[1] > 0
        refcnt = r[1] - 1
        self.set_block_refcnt(block_id, refcnt)
        return refcnt > 0

    def set_block_name(self, block_id, n):
        """ Set name of the block 'block_id' to 'n'.

        This does not change reference counts. """
        raise NotImplementedError

    def set_block_refcnt(self, block_id, refcnt):
        """ Set reference count of the 'block_id' to 'refcnt'.

        If refcnt is zero, the block should be removed.
        """
        raise NotImplementedError

    def store_block(self, block_id, block_data, *, refcnt=1):
        """Store a block with the given block identifier. Note that it is an
error to call this for already existing block; refer_block or
set_block_refcnt should be called instead in that case."""
        raise NotImplementedError


class SQLiteStorage(Storage):
    """For testing purposes, SQLite backend. It does not probably perform
THAT well but can be used to ensure things work correctly and as an
added bonus has in-memory mode.

TBD: using prdb code for this would have been 'nice' but I rather not
mix the two hobby projects for now..

    """

    def __init__(self, name=':memory:', **kw):
        self.conn = sqlite3.connect(name)
        self._get_execute_result(
            'CREATE TABLE blocks(id PRIMARY KEY, data, refcnt);')
        self._get_execute_result(
            'CREATE TABLE blocknames (name PRIMARY KEY, id);')
        Storage.__init__(self, **kw)

    def _get_execute_result(self, q, a=None, ignore_errors=False):
        _debug('_get_execute_result %s %s', q, a)
        c = self.conn.cursor()
        try:
            if a:
                c.execute(q, a)
            else:
                c.execute(q)
        except:
            if ignore_errors:
                return
            else:
                raise
        r = c.fetchall()
        _debug(' => %s', r)
        return r

    def get_block_by_id(self, block_id):
        _debug('get_block_by_id %s', block_id)
        r = self._get_execute_result(
            'SELECT data,refcnt FROM blocks WHERE id=?', (block_id,))
        if len(r) == 0:
            return
        assert len(r) == 1
        return r[0]

    def get_block_id_by_name(self, n):
        _debug('get_block_id_by_name %s', n)
        r = self._get_execute_result(
            'SELECT id FROM blocknames WHERE name=?', (n,))
        if len(r) == 0:
            return
        assert len(r) == 1
        return r[0][0]

    def set_block_name(self, block_id, n):
        _debug('set_block_name %s %s', block_id, n)
        self._get_execute_result(
            'DELETE FROM blocknames WHERE name=?', (n,))
        self._get_execute_result(
            'INSERT INTO blocknames VALUES (?, ?)', (n, block_id))

    def set_block_refcnt(self, block_id, refcnt):
        _debug('release_block %s', block_id)

        # TBD: this is not threadsafe, but what sort of sane person
        # would multithread this backend anyway?
        self._get_execute_result(
            'UPDATE blocks SET refcnt=? WHERE id=?', (refcnt, block_id,))
        if refcnt:
            return
        self.on_delete_block_id(block_id)
        self._get_execute_result(
            'DELETE FROM blocks WHERE id=? and refcnt=0', (block_id,))

    def store_block(self, block_id, block_data, *, refcnt=1):
        _debug('store_block %s %s', block_id, block_data)
        assert self.get_block_data_by_id(block_id) is None
        assert block_data is not None
        self.on_add_block_data(block_data)
        self._get_execute_result(
            'INSERT INTO blocks VALUES (?, ?, ?)', (block_id, block_data,
                                                    refcnt))


class DelayedStorage(Storage):

    """In-memory storage handling; cache reads (up to a point), store the
'writes' for later flush operation."""

    def __init__(self, storage, **kw):
        Storage.__init__(self, **kw)
        self.storage = storage
        self._names = {}  # name -> current, orig

        self._blocks = {}
        # id -> (data, current-refcnt), orig-refcnt, last-access

        # 'cache' = already on disk
        # 'dirty' = not on disk

        self.maximum_cache_size = 0  # kept over flush()es
        self.maximum_dirty_size = 0  # triggers immediate write when exceeded
        self.cache_size = 0
        self.dirty_size = 0

    def _get_block_by_id(self, block_id):
        if block_id not in self._blocks:
            r = self.storage.get_block_by_id(block_id) or (None, 0)
            if r[0] is not None:
                if self.maximum_cache_size and self.cache_size > self.maximum_cache_size:
                    self.flush()
                self.cache_size += len(r[0])
            self._blocks[block_id] = [[r[0], r[1]], r[1], 0, 0]
        r = self._blocks[block_id]
        r[2] = time.time()
        r[3] += 1
        return r

    def flush(self):
        ops = 0
        for block_id, o in self._blocks.items():
            (block_data, block_refcnt), orig_refcnt, _, _ = o
            if not orig_refcnt and block_refcnt:
                self.storage.store_block(
                    block_id, block_data, refcnt=block_refcnt)
            elif block_refcnt != orig_refcnt:
                self.storage.set_block_refcnt(block_id, block_refcnt)
            else:
                continue
            ops += 1
            o[1] = block_refcnt
        for block_name, o in self._names.items():
            (current_id, orig_id) = o
            if current_id != orig_id:
                self.storage.set_block_name(current_id, block_name)
                o[1] = o[0]
                ops += 1
        self.cache_size += self.dirty_size
        self.dirty_size = 0
        if self.maximum_cache_size and self.cache_size > self.maximum_cache_size:
            goal = self.maximum_cache_size * 3 / 4
            # try to stay within [3/4 * max, max]
            l = list(self._blocks.items())
            l.sort(key=lambda k: k[1][2])  # last used time
            while l and self.cache_size > goal:
                (block_id, o) = l.pop(0)
                del self._blocks[block_id]
                block_data = o[0][0]
                if block_data:
                    self.cache_size -= len(block_data)
        return ops

    def get_block_by_id(self, block_id):
        r = self._get_block_by_id(block_id)[0]
        if not r[1]:  # no refcnt -> no object
            return
        return r

    def _get_block_id_by_name(self, n):
        if n not in self._names:
            block_id = self.storage.get_block_id_by_name(n)
            self._names[n] = [block_id, block_id]
        return self._names[n]

    def get_block_id_by_name(self, n):
        return self._get_block_id_by_name(n)[0]

    def set_block_name(self, block_id, n):
        self._get_block_id_by_name(n)[0] = block_id

    def set_block_refcnt(self, block_id, refcnt):
        r = self._get_block_by_id(block_id)
        if refcnt:
            r[0][1] = refcnt
            return
        self.on_delete_block_id(block_id)
        r[0][1] = 0
        if not r[1]:
            self.dirty_size -= len(r[0][0])
        else:
            self.cache_size -= len(r[0][0])
        r[0][0] = None

    def store_block(self, block_id, block_data, *, refcnt=1):
        r = self._get_block_by_id(block_id)
        assert r[0][0] is None
        assert block_data
        self.on_add_block_data(block_data)
        r[0][0] = block_data
        r[0][1] = refcnt
        self.dirty_size += len(block_data)
        if self.maximum_dirty_size and self.dirty_size > self.maximum_dirty_size:
            self.flush()
