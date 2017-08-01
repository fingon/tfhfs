#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: storage_lmdb.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2017 Markus Stenberg
#
# Created:       Tue Aug  1 18:04:55 2017 mstenber
# Last modified: Tue Aug  1 18:33:31 2017 mstenber
# Edit time:     22 min
#
"""

Somewhat more performant storage system than sqlite, as the sql itself
does not really help much here anyway.

"""

import logging
import sys
import tempfile

import cbor
import lmdb
import psutil

import storage

_debug = logging.getLogger(__name__).debug


class LMDBStorageBackend(storage.StorageBackend):
    """LMDB storage backend.

    It does not probably perform THAT well but can be used to ensure
    things work correctly and as an added bonus has in-memory mode.

    TBD: using prdb code for this would have been 'nice' but I rather not
    mix the two hobby projects for now..
    """

    def __init__(self, *, codec=None, filename=None, **kw):
        if filename is None:
            self.tempdir = tempfile.TemporaryDirectory()
            filename = self.tempdir.name
        self.filename = filename
        self.codec = codec or storage.NopBlockCodec()
        self.env = lmdb.open(self.filename, max_dbs=3,
                             metasync=False,  # system crash may undo last committed transaction
                             sync=False, writemap=False,  # ACI but no D
                             map_size=1 << 40,
                             )
        self.name_db = self.env.open_db(b'name2id')
        self.block_db = self.env.open_db(b'id2block')

    @property
    def block_id_key(self):
        return self.codec.block_id_key

    def delete_block(self, block):
        _debug('delete_block %s', block)
        with self.env.begin(db=self.block_db, write=True) as t:
            t.delete(block.id)

    def flush_block(self, block):
        data = cbor.dumps((block.data, block.refcnt, block.type))
        with self.env.begin(db=self.block_db, write=True) as t:
            if t.get(block.id) == data:
                return 0
            t.put(block.id, data)
            return 1

    def flush_done(self):
        self.env.sync()

    def get_block_by_id(self, st, block_id):
        _debug('get_block_by_id %s', block_id)
        with self.env.begin(db=self.block_db) as t:
            r = t.get(block_id)
            if not r:
                return
            r = cbor.loads(r)
        block_data, block_refcnt, block_type = r
        block_data = self.codec.decode_block(block_id, block_data)
        b = storage.StoredBlock(st, block_id, refcnt=block_refcnt, data=block_data,
                                type=block_type)
        return b

    def get_block_id_by_name(self, n):
        _debug('get_block_id_by_name %s', n)
        with self.env.begin(db=self.name_db) as t:
            return t.get(n)

    def get_bytes_available(self):
        if self.filename == ':memory:':
            return psutil.virtual_memory().available
        return psutil.disk_usage(self.filename).free

    def get_bytes_used(self):
        st = self.env.stat()
        return st['psize'] * (st['branch_pages'] + st['leaf_pages'] + st['overflow_pages'])

    def set_block_name(self, block_id, n):
        _debug('set_block_name_raw %s %s', block_id, n)
        assert n
        with self.env.begin(db=self.name_db, write=True) as t:
            if block_id:
                t.put(n, block_id)
            else:
                t.delete(n)

    def store_block(self, block):
        _debug('store_block %s', block)
        assert self.get_block_by_id(block.parent, block.id) is None
        self.flush_block(block)


class LMDBStorage(storage.Storage):
    backend_class = LMDBStorageBackend
