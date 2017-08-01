#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: storage_sqlite.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2017 Markus Stenberg
#
# Created:       Tue Aug  1 17:54:38 2017 mstenber
# Last modified: Tue Aug  1 17:59:59 2017 mstenber
# Edit time:     2 min
#
"""

Moved sqlite specific storage from the generic storage module.

This is not super efficient, but can be used for amusement value.

"""

import logging
import sqlite3

import psutil

import storage

_debug = logging.getLogger(__name__).debug


class SQLiteStorageBackend(storage.StorageBackend):
    """SQLite storage backend.

    It does not probably perform THAT well but can be used to ensure
    things work correctly and as an added bonus has in-memory mode.

    TBD: using prdb code for this would have been 'nice' but I rather not
    mix the two hobby projects for now..
    """

    def __init__(self, *, codec=None, filename=':memory:', **kw):
        self.filename = filename
        self.codec = codec or storage.NopBlockCodec()
        self.conn = sqlite3.connect(filename, check_same_thread=False)
        self._get_execute_result(
            'CREATE TABLE IF NOT EXISTS blocks(id PRIMARY KEY, data, refcnt, type);')
        self._get_execute_result(
            'CREATE INDEX IF NOT EXISTS block_type ON blocks (type);')
        self._get_execute_result(
            'CREATE TABLE IF NOT EXISTS blocknames (name PRIMARY KEY, id);')

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

    @property
    def block_id_key(self):
        return self.codec.block_id_key

    def delete_block(self, block):
        _debug('delete_block %s', block)
        self._get_execute_result(
            'DELETE FROM blocks WHERE id=?', (block.id,))

    def _get_changed_block_fields(self, block):
        prev_block = block.stored
        if not prev_block or (not block.data) != (not prev_block.data):
            data = block.data
            if not data:
                data = b''
            else:
                data = self.codec.encode_block(block.id, data)
            yield ('data', data)
        if not prev_block or block.refcnt != prev_block.refcnt:
            yield 'refcnt', block.refcnt
        if not prev_block or block.type != prev_block.type:
            yield 'type', block.type

    def flush_block(self, block):
        params = []
        fields = []
        for k, v in self._get_changed_block_fields(block):
            fields.append('%s=?' % k)
            params.append(v)
        params.append(block.id)
        fields = ','.join(fields)
        if len(params) > 1:
            self._get_execute_result(
                'UPDATE blocks SET %s WHERE id=?' % fields, tuple(params))
        return len(params) - 1

    def flush_done(self):
        self.conn.commit()

    def get_block_by_id(self, st, block_id):
        _debug('get_block_by_id %s', block_id)
        r = self._get_execute_result(
            'SELECT data,refcnt,type FROM blocks WHERE id=?', (block_id,))
        if len(r) == 0:
            return
        assert len(r) == 1
        r = list(r[0])
        block_data, block_refcnt, block_type = r
        block_data = self.codec.decode_block(block_id, block_data)
        b = storage.StoredBlock(st, block_id, refcnt=block_refcnt, data=block_data,
                                type=block_type)
        return b

    def get_block_id_by_name(self, n):
        _debug('get_block_id_by_name %s', n)
        r = self._get_execute_result(
            'SELECT id FROM blocknames WHERE name=?', (n,))
        if len(r) == 0:
            return
        assert len(r) == 1
        return r[0][0]

    def get_bytes_available(self):
        if self.filename == ':memory:':
            return psutil.virtual_memory().available
        return psutil.disk_usage(self.filename).free

    def get_bytes_used(self):
        r1 = self._get_execute_result('PRAGMA page_count;')
        r2 = self._get_execute_result('PRAGMA page_size;')
        return r1[0][0] * r2[0][0]

    def set_block_name(self, block_id, n):
        assert n
        _debug('set_block_name_raw %s %s', block_id, n)
        self._get_execute_result(
            'DELETE FROM blocknames WHERE name=?', (n,))
        if block_id:
            self._get_execute_result(
                'INSERT INTO blocknames VALUES (?, ?)', (n, block_id))

    def store_block(self, block):
        _debug('store_block %s', block)
        assert self.get_block_by_id(block.parent, block.id) is None
        fields = ['id']
        params = [block.id]
        for k, v in self._get_changed_block_fields(block):
            fields.append(k)
            params.append(v)
        fields = ', '.join(fields)
        qs = ', '.join(['?'] * len(params))
        self._get_execute_result(
            'INSERT INTO blocks (%s) VALUES (%s)' % (fields, qs), tuple(params))


class SQLiteStorage(storage.Storage):
    backend_class = SQLiteStorageBackend
