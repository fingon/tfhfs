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
# Last modified: Fri Dec 16 08:48:19 2016 mstenber
# Edit time:     392 min
#
"""This is the 'storage layer' main module.

It provides an abstract interface for the forest layer to use, and
uses storage backend for actual raw file operations.

TBD: how to handle maximum_cache_size related flushes? trigger timer
to flush earlier? within call stack, it is bad idea (to some extent
this applies also to the maximum_dirty_size)

"""

import logging
import os
import sqlite3
import time

import psutil
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

import const
import lz4
from endecode import Decoder, Encoder
from util import getrecsizeof

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


class _NopIterator:

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration

_nopiterator = _NopIterator()


class Storage:

    def block_data_references_callback(self, block_data):
        return _nopiterator

    def delete_block_id_with_deps(self, block_id):
        """Delete block id and remove its references."""
        r = self.get_block_by_id(block_id)
        assert r
        (block_data, block_refcnt) = r
        assert block_data
        for block_id2 in self.get_block_data_references(block_data):
            self.release_block(block_id2)
        self.delete_block_id(block_id)
        return True

    def delete_block_id(self, block_id):
        """Raw deletion on storage. This is up to particular Storage subclasses. """
        raise NotImplementedError

    def get_block_data_references(self, block_data):
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

    def get_bytes_available(self):
        raise NotImplementedError

    def get_bytes_used(self):
        raise NotImplementedError

    def on_add_block_data(self, block_data):
        for block_id in self.get_block_data_references(block_data):
            _debug('on_add_block_data add reference to %s', block_id)
            self.refer_block(block_id)

    def refer_block(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r
        self.set_block_refcnt(block_id, r[1] + 1)

    def refer_or_store_block(self, block_id, block_data):
        """Convenience method for handling the common case of 'we have these
bytes, no clue if the block is already inserted (by someone else) ->
either refer to existing one, or add new block to the storage
layer."""
        r = self.get_block_by_id(block_id)
        if r:
            self.refer_block(block_id)
        else:
            self.store_block(block_id, block_data)

    def release_block(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r
        refcnt = r[1] - 1
        assert refcnt >= 0
        self.set_block_refcnt(block_id, refcnt)
        return refcnt > 0

    def set_block_data_references_callback(self, callback):
        self.block_data_references_callback = callback

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


class ReferringStorage(Storage):
    """Storage subclass with external references. Typically, only the
    low-level storage (=one which uses disk directly) should bother with them.

    e.g. caching storage layer should be able to ignore this part of
    Storage logic.
    """
    referenced_refcnt0_block_ids = None

    def __init__(self, block_id_has_references_callback=None, **kw):
        self.set_block_id_has_references_callback(
            block_id_has_references_callback)
        Storage.__init__(self, **kw)

    def block_id_has_references_callback(self, block_id):
        pass

    def delete_block_id_if_no_extref(self, block_id):
        """This is the main delete function and should be called if the class
        cares about external dependencies. If external dependencies
        exist, this function will be eventually called by later flush
        methods until they do not exist.
        """
        if self.block_id_has_references_callback(block_id):
            if not self.referenced_refcnt0_block_ids:
                self.referenced_refcnt0_block_ids = set()
            self.referenced_refcnt0_block_ids.add(block_id)
            return
        return self.delete_block_id_with_deps(block_id)

    def flush(self):
        """ Attempt to get rid of dangling reference count 0 blocks. """
        while self.referenced_refcnt0_block_ids:
            s = self.referenced_refcnt0_block_ids
            del self.referenced_refcnt0_block_ids
            deleted = False
            for block_id in s:
                (data, refcnt) = self.get_block_by_id(block_id)
                if not refcnt:
                    if self.delete_block_id_if_no_extref(block_id):
                        deleted = True
            if not deleted:
                return

    def set_block_id_has_references_callback(self, callback):
        self.block_id_has_references_callback = callback
        if callback is None:
            del self.block_id_has_references_callback


class DictStorage(ReferringStorage):
    """ For testing purposes, in-memory dict-based storage. """

    def __init__(self, **kw):
        ReferringStorage.__init__(self, **kw)
        self.name2bid = {}
        self.bid2datarefcnt = {}

    def delete_block_id(self, block_id):
        del self.bid2datarefcnt[block_id]

    def get_block_by_id(self, block_id):
        return self.bid2datarefcnt.get(block_id)

    def get_block_id_by_name(self, n):
        return self.name2bid.get(n)

    def get_bytes_available(self):
        return psutil.virtual_memory().available

    def get_bytes_used(self):
        seen = set()
        return getrecsizeof(self.name2bid, seen) + \
            getrecsizeof(self.bid2datarefcnt, seen)

    def set_block_name_raw(self, block_id, n):
        if block_id:
            self.name2bid[n] = block_id
        else:
            del self.name2bid[n]

    def set_block_refcnt(self, block_id, refcnt):
        self.bid2datarefcnt[block_id][1] = refcnt
        if refcnt:
            return
        self.delete_block_id_if_no_extref(block_id)

    def store_block(self, block_id, block_data, *, refcnt=1):
        self.bid2datarefcnt[block_id] = [block_data, refcnt]


class SQLiteStorage(ReferringStorage):
    """For testing purposes, SQLite backend. It does not probably perform
THAT well but can be used to ensure things work correctly and as an
added bonus has in-memory mode.

TBD: using prdb code for this would have been 'nice' but I rather not
mix the two hobby projects for now..

    """

    def __init__(self, *, codec=None, filename=':memory:', **kw):
        self.filename = filename
        self.codec = codec or NopBlockCodec()
        self.conn = sqlite3.connect(filename)
        self._get_execute_result(
            'CREATE TABLE blocks(id PRIMARY KEY, data, refcnt);')
        self._get_execute_result(
            'CREATE TABLE blocknames (name PRIMARY KEY, id);')
        ReferringStorage.__init__(self, **kw)

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

    def delete_block_id(self, block_id):
        self._get_execute_result(
            'DELETE FROM blocks WHERE id=? and refcnt=0', (block_id,))

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

    def get_bytes_available(self):
        if self.filename == ':memory:':
            return psutil.virtual_memory().available
        return psutil.disk_usage(self.filename).free

    def get_bytes_used(self):
        r1 = self._get_execute_result('PRAGMA page_count;')
        r2 = self._get_execute_result('PRAGMA page_size;')
        return r1[0][0] * r2[0][0]

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
        self.delete_block_id_if_no_extref(block_id)

    def store_block(self, block_id, block_data, *, refcnt=1):
        _debug('store_block %s %s', block_id, block_data)
        assert self.get_block_data_by_id(block_id) is None
        assert block_data is not None
        self.on_add_block_data(block_data)
        e_block_data = self.codec.encode_block(block_id, block_data)
        self._get_execute_result(
            'INSERT INTO blocks VALUES (?, ?, ?)', (block_id, e_block_data,
                                                    refcnt))


class DelayedStorageItem:

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, vars(self))


class DelayedStorage(Storage):

    """In-memory storage handling; cache reads (up to a point), store the
'writes' for later flush operation."""

    def __init__(self, storage, **kw):
        Storage.__init__(self, **kw)
        assert isinstance(storage, ReferringStorage)
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
            r = self.storage.get_block_by_id(block_id)
            if r:
                self.cache_size += len(r[0])
                orig_refcnt = r[1]
            else:
                r = (None, 0)
                orig_refcnt = None
            self._blocks[block_id] = DelayedStorageItem(data_refcnt=[r[0], r[1]],
                                                        orig_refcnt=orig_refcnt,
                                                        t=0, cnt=0)
        r = self._blocks[block_id]
        r.t = time.time()
        r.cnt += 1
        return r

    def _flush_blocks(self, positive):
        ops = 0
        for block_id, o in sorted(self._blocks.items()):
            (block_data, block_refcnt) = o.data_refcnt
            orig_refcnt = o.orig_refcnt or 0

            # The order here is frightfully important!
            # The following 3 steps do not work in any other order.
            # (Why? Left as an exercise to the reader)
            if positive is not None and ((block_refcnt < orig_refcnt) == (not positive)):
                continue
            if block_data and not block_refcnt:
                self.cache_size -= len(block_data)
                o.data_refcnt[0] = None
            if block_refcnt == orig_refcnt:
                continue
            if not orig_refcnt and block_refcnt:
                self.storage.store_block(block_id, block_data,
                                         refcnt=block_refcnt)
            else:
                self.storage.set_block_refcnt(block_id, block_refcnt)
            ops += 1
            o.orig_refcnt = block_refcnt
        return ops

    @property
    def calculated_cache_size(self):
        v = 0
        for o in self._blocks.values():
            (block_data, block_refcnt) = o.data_refcnt
            if block_data:
                v += len(block_data)
        return v

    @property
    def calculated_dirty_size(self):
        v = 0
        for o in self._blocks.values():
            (block_data, block_refcnt) = o.data_refcnt
            if block_data and not o.orig_refcnt and block_refcnt:
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
        l.sort(key=lambda k: k[1].t)  # last used time
        while l and self.cache_size > goal:
            (block_id, o) = l.pop(0)
            self._delete_cached_block_id(block_id)

    def _delete_cached_block_id(self, block_id):
        o = self._blocks.pop(block_id)
        orig_refcnt = o.orig_refcnt or 0
        assert o.data_refcnt[1] == orig_refcnt  # in cache -> should be
        block_data = o.data_refcnt[0]
        if block_data:
            self.cache_size -= len(block_data)

    def delete_block_id(self, block_id):
        pass

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
        # Also call underlying storage's flush method in case some of this was
        # delayed
        if hasattr(self.storage, 'flush'):
            self.storage.flush()
            # If it did, any local references with 0 refcnt may be gone.
            # Remove them just in case.
            for block_id, o in list(self._blocks.items()):
                if not o.data_refcnt[1]:
                    self._delete_cached_block_id(block_id)
        return ops

    def get_block_by_id(self, block_id):
        _debug('%s.get_block_by_id %s', self.__class__.__name__, block_id)
        r = self._get_block_by_id(block_id)
        _debug(' => %s', r)
        if not r.data_refcnt[1] and not self.storage.block_id_has_references_callback(block_id):
            _debug(' [skip]')
            return
        return r.data_refcnt

    def _get_block_id_by_name(self, n):
        if n not in self._names:
            block_id = self.storage.get_block_id_by_name(n)
            self._names[n] = [block_id, block_id]
        return self._names[n]

    def get_block_id_by_name(self, n):
        return self._get_block_id_by_name(n)[0]

    def get_bytes_available(self):
        return self.storage.get_bytes_available()

    def get_bytes_used(self):
        return self.storage.get_bytes_used()

    # def set_block_data_references_callback(self, callback):
    #    self.storage.set_block_data_references_callback(callback)
    # TBD: Should we do it here, or not?
    # If not, we need some sort of reference count change callback
    # to propagate to here. If yes, we need to fix delete case :p
    # (right now only add is covered)

    def set_block_id_has_references_callback(self, callback):
        self.storage.set_block_id_has_references_callback(callback)

    def set_block_name_raw(self, block_id, n):
        self._get_block_id_by_name(n)[0] = block_id

    def set_block_refcnt(self, block_id, refcnt):
        _debug('%s.set_block_refcnt %s = %d',
               self.__class__.__name__, block_id, refcnt)
        r = self._get_block_by_id(block_id)
        assert r
        if not refcnt:
            self.delete_block_id_with_deps(block_id)
            # We do not intentionally call delete_block_id, as
            # underlying storage should have the information it needs
            # and maintaining two implementations which check external
            # references is inefficient and it is the underlying
            # storage which counts.

        r.data_refcnt[1] = refcnt
        _debug(' => %s [set_block_refcnt]', r)

    def store_block(self, block_id, block_data, *, refcnt=1):
        _debug('store_block %s', block_id)
        r = self._get_block_by_id(block_id)
        assert r.data_refcnt[0] is None
        assert block_data
        self.on_add_block_data(block_data)
        r.data_refcnt = [block_data, refcnt]
        self.dirty_size += len(block_data)
        self.cache_size += len(block_data)
        if (self.maximum_dirty_size and
                self.dirty_size > self.maximum_dirty_size):
            self.flush()
