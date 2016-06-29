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
# Last modified: Wed Jun 29 10:42:52 2016 mstenber
# Edit time:     23 min
#
"""

This is the 'storage layer' main module.

It is provides abstract interface for the forest layer to use, and
uses storage backend for actual raw file operations.

"""

import sqlite3

import logging

_debug = logging.getLogger(__name__).debug


class Storage:

    def get_block_by_id(self, block_id):
        """Get data for the block identified by block_id. If the block does
not exist, None is returned."""

    def get_block_id_by_name(self, n):
        """Get the block identifier for the named block. If the name is not
set, None is returned."""

    def release_block(self, block_id):
        """ Release reference to a block. """

    def set_block_id_name(self, block_id, n):
        """ Set name of the block 'block_id' to 'n'.

        This does not change reference counts. """

    def store_block(self, block_id, block_data=None):
        """ Store a block with the given block identifier.

        If it already exists, increment reference count by 1.
        """


class SQLiteStorage(Storage):
    """For testing purposes, SQLite backend. It does not probably perform
THAT well but can be used to ensure things work correctly and as an
added bonus has in-memory mode.

TBD: using prdb code for this would have been 'nice' but I rather not
mix the two hobby projects for now..

    """

    def __init__(self, name=':memory:'):
        self.conn = sqlite3.connect(name)
        self._get_execute_result(
            'CREATE TABLE blocks(id PRIMARY KEY, data, refcnt);')
        self._get_execute_result(
            'CREATE TABLE blocknames (name PRIMARY KEY, id);')

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
            'SELECT data FROM blocks WHERE id=?', (block_id,))
        if len(r) == 0:
            return
        assert len(r) == 1
        return r[0][0]

    def get_block_id_by_name(self, n):
        _debug('get_block_id_by_name %s', n)
        r = self._get_execute_result(
            'SELECT id FROM blocknames WHERE name=?', (n,))
        if len(r) == 0:
            return
        assert len(r) == 1
        return r[0][0]

    def release_block(self, block_id):
        _debug('release_block %s', block_id)
        self._get_execute_result(
            'UPDATE blocks SET refcnt=refcnt-1 WHERE id=?', (block_id,))
        self._get_execute_result(
            'DELETE FROM blocks WHERE id=? and refcnt=0', (block_id,))

    def set_block_id_name(self, block_id, n):
        _debug('set_block_id_name %s %s', block_id, n)
        self._get_execute_result(
            'DELETE FROM blocknames WHERE name=?', (n,))
        self._get_execute_result(
            'INSERT INTO blocknames VALUES (?, ?)', (n, block_id))

    def store_block(self, block_id, block_data=None):
        _debug('store_block %s %s', block_id, block_data)
        if self.get_block_by_id(block_id) is not None:
            self._get_execute_result(
                'UPDATE blocks SET refcnt=refcnt+1 WHERE id=?', (block_id,))
        else:
            assert block_data is not None
            self._get_execute_result(
                'INSERT INTO blocks VALUES (?, ?, 1)', (block_id, block_data))
