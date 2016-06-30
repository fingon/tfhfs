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
# Last modified: Thu Jun 30 15:27:49 2016 mstenber
# Edit time:     142 min
#
"""

This is the 'storage layer' main module.

It is provides abstract interface for the forest layer to use, and
uses storage backend for actual raw file operations.

"""

import sqlite3
import time
import logging
import os

import lz4

from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

_debug = logging.getLogger(__name__).debug


class BlockCodec:
    """This is a class which handles the raw encode-decode of block
data. This is not yet type-specific, but may e.g. do
compression+encryption."""

    def encode_block(self, block_id, block_data):
        raise NotImplementedError

    def decode_block(self, block_id, block_data):
        raise NotImplementedError


class NopBlockCodec(BlockCodec):

    def encode_block(self, block_id, block_data):
        return block_data

    def decode_block(self, block_id, block_data):
        return block_data


class ConfidentialBlockCodec(BlockCodec):
    """Real class which implements the (outer layer) of block codec
stuff, which provides for confidentiality and authentication of the
data within.."""
    magic = b'4207'
    block_id_len = 32
    iv_len = 16
    tag_len = 16

    def __init__(self, password):
        self.backend = default_backend()
        # TBD: is empty salt ever ok? :p
        salt = b''
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(),
                         length=32,
                         salt=salt,
                         iterations=100000,
                         backend=self.backend)
        self.key = kdf.derive(password)

    def encode_block(self, block_id, block_data):
        assert (isinstance(block_id, bytes)
                and len(block_id) == self.block_id_len)
        iv = os.urandom(self.iv_len)
        c = Cipher(algorithms.AES(self.key), modes.GCM(iv),
                   backend=self.backend)
        e = c.encryptor()
        s = e.update(block_id) + e.update(block_data) + e.finalize()
        l = [self.magic, iv, e.tag, s]
        assert len(e.tag) == self.tag_len
        return b''.join(l)

    def decode_block(self, block_id, block_data):
        assert (isinstance(block_id, bytes)
                and len(block_id) == self.block_id_len)
        assert isinstance(block_data, bytes)
        assert len(block_data) > (len(self.magic) + self.iv_len + self.tag_len)

        # check magic
        ofs = len(self.magic)
        assert block_data[:ofs] == self.magic

        # get iv
        iv = block_data[ofs:ofs + self.iv_len]
        ofs += self.iv_len

        # get tag
        tag = block_data[ofs:ofs + self.tag_len]
        ofs += self.tag_len

        c = Cipher(algorithms.AES(self.key), modes.GCM(iv, tag),
                   backend=self.backend)
        d = c.decryptor()
        s = d.update(block_data[ofs:]) + d.finalize()
        assert s[:self.block_id_len] == block_id
        return s[self.block_id_len:]


class TypedBlockCodec(BlockCodec):

    def __init__(self, codec):
        self.codec = codec

    def encode_block(self, block_id, block_data):
        (t, d) = block_data
        assert isinstance(t, int) and t >= 0 and t < 256  # just one byte
        assert isinstance(d, bytes)
        return self.codec.encode_block(block_id, bytes([t]) + d)

    def decode_block(self, block_id, block_data):
        assert isinstance(block_data, bytes)
        assert len(block_data) > 0
        rd = self.codec.decode_block(block_id, block_data)
        assert len(rd) > 0
        return (rd[0], rd[1:])


class CompressingTypedBlockCodec(TypedBlockCodec):

    bit_compressed = 0x80

    def encode_block(self, block_id, block_data):
        (t, d) = block_data
        assert not (t & self.bit_compressed)
        cd = lz4.compress(d)
        if len(cd) < len(d):
            t = t | self.bit_compressed
            d = cd
        return TypedBlockCodec.encode_block(self, block_id, (t, d))

    def decode_block(self, block_id, block_data):
        (t, d) = TypedBlockCodec.decode_block(self, block_id, block_data)
        if t & self.bit_compressed:
            d = lz4.loads(d)
            t = t & ~self.bit_compressed
        return (t, d)


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
            if block_refcnt == orig_refcnt:
                continue
            if not orig_refcnt and block_refcnt:
                self.storage.store_block(block_id, block_data,
                                         refcnt=block_refcnt)
            else:
                self.storage.set_block_refcnt(block_id, block_refcnt)
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
