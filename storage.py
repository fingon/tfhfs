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
# Last modified: Sat Jul  2 21:16:31 2016 mstenber
# Edit time:     259 min
#
"""This is the 'storage layer' main module.

It is provides abstract interface for the forest layer to use, and
uses storage backend for actual raw file operations.

TBD: how to handle maximum_cache_size related flushes? trigger timer
to flush earlier? within call stack, it is bad idea (to some extent
this applies also to the maximum_dirty_size)

"""

import logging
import os
import sqlite3
import time

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

import const
import lz4
from endecode import Decoder, Encoder

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
        enc = Encoder()
        enc.encode_bytes(self.magic)
        iv = os.urandom(self.iv_len)
        enc.encode_bytes(iv)
        c = Cipher(algorithms.AES(self.key), modes.GCM(iv),
                   backend=self.backend)
        e = c.encryptor()
        e.authenticate_additional_data(block_id)
        s = e.update(block_data) + e.finalize()
        assert len(e.tag) == self.tag_len
        enc.encode_bytes(e.tag)
        enc.encode_bytes(s)
        return enc.value

    def decode_block(self, block_id, block_data):
        assert (isinstance(block_id, bytes)
                and len(block_id) == self.block_id_len)
        assert isinstance(block_data, bytes)
        assert len(block_data) > (len(self.magic) + self.iv_len + self.tag_len)

        dec = Decoder(block_data)

        # check magic
        assert dec.decode_bytes(len(self.magic)) == self.magic

        # get iv
        iv = dec.decode_bytes(self.iv_len)

        # get tag
        tag = dec.decode_bytes(self.tag_len)

        c = Cipher(algorithms.AES(self.key), modes.GCM(iv, tag),
                   backend=self.backend)
        d = c.decryptor()
        d.authenticate_additional_data(block_id)

        s = d.update(dec.decode_bytes_rest()) + d.finalize()
        return s


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

    def encode_block(self, block_id, block_data):
        (t, d) = block_data
        assert not (t & const.BIT_COMPRESSED)
        cd = lz4.compress(d)
        if len(cd) < len(d):
            t = t | const.BIT_COMPRESSED
            d = cd
        return TypedBlockCodec.encode_block(self, block_id, (t, d))

    def decode_block(self, block_id, block_data):
        (t, d) = TypedBlockCodec.decode_block(self, block_id, block_data)
        if t & const.BIT_COMPRESSED:
            d = lz4.loads(d)
            t = t & ~const.BIT_COMPRESSED
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
            _debug('on_add_block_data add reference to %s', block_id)
            self.refer_block(block_id)

    def on_delete_block_id(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r
        (block_data, block_refcnt) = r
        assert block_data
        for block_id2 in self.get_block_data_references(block_data):
            _debug('on_delete_block_id %s: drop to %s', block_id, block_id2)
            self.release_block(block_id2)

    def refer_block(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r
        self.set_block_refcnt(block_id, r[1] + 1)

    def release_block(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r
        refcnt = r[1] - 1
        assert refcnt >= 0
        self.set_block_refcnt(block_id, refcnt)
        return refcnt > 0

    def set_block_name(self, block_id, n):
        old_block_id = self.get_block_id_by_name(n)
        if old_block_id == block_id:
            return
        if block_id:
            self.refer_block(block_id)
        if old_block_id:
            self.release_block(old_block_id)
        self.set_block_name_raw(block_id, n)

    def set_block_name_raw(self, block_id, n):
        """Set name of the block 'block_id' to 'n'.

        This does not change reference counts and therefore should be
        mostly used by the internal APIs.
        """
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

    def __init__(self, *, codec=None, filename=':memory:', **kw):
        self.codec = codec or NopBlockCodec()
        self.conn = sqlite3.connect(filename)
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
        return [self.codec.decode_block(block_id, r[0][0]), r[0][1]]

    def get_block_id_by_name(self, n):
        _debug('get_block_id_by_name %s', n)
        r = self._get_execute_result(
            'SELECT id FROM blocknames WHERE name=?', (n,))
        if len(r) == 0:
            return
        assert len(r) == 1
        return r[0][0]

    def set_block_name_raw(self, block_id, n):
        assert n
        _debug('set_block_name_raw %s %s', block_id, n)
        self._get_execute_result(
            'DELETE FROM blocknames WHERE name=?', (n,))
        if block_id:
            self._get_execute_result(
                'INSERT INTO blocknames VALUES (?, ?)', (n, block_id))

    def set_block_refcnt(self, block_id, refcnt):
        _debug('set_block_refcnt %s = %d', block_id, refcnt)

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
        e_block_data = self.codec.encode_block(block_id, block_data)
        self._get_execute_result(
            'INSERT INTO blocks VALUES (?, ?, ?)', (block_id, e_block_data,
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
                self.cache_size += len(r[0])
            self._blocks[block_id] = [[r[0], r[1]], r[1], 0, 0]
        r = self._blocks[block_id]
        r[2] = time.time()
        r[3] += 1
        return r

    def _flush_blocks(self, positive):
        ops = 0
        for block_id, o in sorted(self._blocks.items()):
            (block_data, block_refcnt), orig_refcnt, _, _ = o

            # The order here is frightfully important!
            # The following 3 steps do not work in any other order.
            # (Why? Left as an exercise to the reader)
            if positive is not None and ((block_refcnt < orig_refcnt) == (not positive)):
                continue
            if block_data and not block_refcnt:
                self.cache_size -= len(block_data)
                o[0][0] = None
            if block_refcnt == orig_refcnt:
                continue
            if not orig_refcnt and block_refcnt:
                self.storage.store_block(block_id, block_data,
                                         refcnt=block_refcnt)
            else:
                self.storage.set_block_refcnt(block_id, block_refcnt)
            ops += 1
            o[1] = block_refcnt  # replace orig_refcnt with the updated one
        return ops

    @property
    def calculated_cache_size(self):
        v = 0
        for o in self._blocks.values():
            (block_data, block_refcnt), orig_refcnt, _, _ = o
            if block_data:
                v += len(block_data)
        return v

    @property
    def calculated_dirty_size(self):
        v = 0
        for o in self._blocks.values():
            (block_data, block_refcnt), orig_refcnt, _, _ = o
            if block_data and not orig_refcnt and block_refcnt:
                v += len(block_data)
        return v

    def _flush_names(self):
        ops = 0
        for block_name, o in self._names.items():
            (current_id, orig_id) = o
            if current_id != orig_id:
                self.storage.set_block_name_raw(current_id, block_name)
                o[1] = o[0]
                ops += 1
        return ops

    def _shrink_cache(self):
        goal = self.maximum_cache_size * 3 // 4
        _debug('_shrink_cache goal=%d < %d', goal, self.cache_size)
        # try to stay within [3/4 * max, max]
        l = list(self._blocks.items())
        l.sort(key=lambda k: k[1][2])  # last used time
        while l and self.cache_size > goal:
            (block_id, o) = l.pop(0)
            del self._blocks[block_id]
            assert o[1] == o[0][1]  # refcnt must be constant
            block_data = o[0][0]
            if block_data:
                self.cache_size -= len(block_data)

    def flush(self):
        _debug('flush')
        ops = 0
        ops += self._flush_blocks(1)
        self.dirty_size = 0  # new blocks if any are written to disk by now
        ops += self._flush_names()
        ops += self._flush_blocks(None)
        if self.maximum_cache_size and self.cache_size > self.maximum_cache_size:
            self._shrink_cache()
        _debug(' => %d ops', ops)
        return ops

    def get_block_by_id(self, block_id):
        _debug('%s.get_block_by_id %s', self.__class__.__name__, block_id)
        r = self._get_block_by_id(block_id)[0]
        _debug(' => %s', r)
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

    def set_block_name_raw(self, block_id, n):
        self._get_block_id_by_name(n)[0] = block_id

    def set_block_refcnt(self, block_id, refcnt):
        _debug('%s.set_block_refcnt %s = %d',
               self.__class__.__name__, block_id, refcnt)
        r = self._get_block_by_id(block_id)
        assert r
        if not refcnt:
            self.on_delete_block_id(block_id)
        r[0][1] = refcnt
        _debug(' => %s', r)

    def store_block(self, block_id, block_data, *, refcnt=1):
        _debug('store_block %s', block_id)
        r = self._get_block_by_id(block_id)
        assert r[0][0] is None
        assert block_data
        self.on_add_block_data(block_data)
        r[0][0] = block_data
        r[0][1] = refcnt
        self.dirty_size += len(block_data)
        self.cache_size += len(block_data)
        if (self.maximum_dirty_size and
                self.dirty_size > self.maximum_dirty_size):
            self.flush()
