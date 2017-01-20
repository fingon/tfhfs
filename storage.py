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
# Last modified: Fri Jan 20 18:44:08 2017 mstenber
# Edit time:     864 min
#
"""This is the 'storage layer' main module.

It provides an abstract interface for the forest layer to use, and
uses storage backend for actual raw file operations.

TBD: how to handle maximum_cache_size related flushes? trigger timer
to flush earlier? within call stack, it is bad idea?

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
import util
from endecode import Decoder, Encoder
from ms.lazy import lazy_property
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

    block_id_key = b''

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

    def __init__(self, password, salt=b''):
        self.backend = default_backend()
        # TBD: is empty salt ever ok? :p
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(),
                         length=32,
                         salt=salt,
                         iterations=100000,
                         backend=self.backend)
        self.key = kdf.derive(password)

    @property
    def block_id_key(self):
        return self.key  # TBD: want to derive key, or use as is?

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

    @property
    def block_id_key(self):
        return self.codec.block_id_key

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


class StoredBlock(util.DirtyMixin):
    """Single block on storage backend (or pending to be inserted to
    storage backend). Notably, only block id (and parent) are truly
    immutable.

    - data may get set or unset (but not to different value)
    - refcnt changes
    - type may change

    """

    stored = None  # stored version of the block

    t = 0  # for LRU use, in cache
    cache_size = 0  # size used in the cache count in e.g. DelayedStorage

    def __init__(self, parent, id, *, refcnt=None, data=None, type=None):
        assert isinstance(parent, Storage)
        self.parent = parent
        self.id = id
        # ^ should be immutable

        self.refcnt = refcnt
        self.data = data
        self.type = type
        # ^ should have accessors for mutation

    def get_cache_delta(self):
        size = util.getrecsizeof(self.data) if self.data else 0
        delta = size - self.cache_size
        self.cache_size = size
        return delta

    def mark_dirty_related(self):
        assert self.stored is None
        self.stored = StoredBlock(self.parent, self.id, refcnt=self.refcnt,
                                  data=self.data, type=self.type)
        self.stored.cache_size = self.cache_size
        if self.parent:
            self.parent.mark_block_dirty(self)

    def perform_flush(self):
        assert isinstance(self.parent, Storage)
        ops = 0
        if self.stored.refcnt is None:
            if self.refcnt:
                self.parent.store_block_be(self)
                ops += 1
            else:
                # don't even remove self.stored - it retains the fact
                # this is not persisted to disk
                return 0
        else:
            # It is stored on disk. Update it there.
            self.parent.updated_block_type(self, self.stored.type, self.type)
            self.parent.flush_block_be(self)
            ops += 1
        del self.stored
        return ops

    def set_refcnt(self, refcnt):
        self.mark_dirty()
        self.refcnt = refcnt

    def set_data(self, data):
        self.mark_dirty()
        self.data = data

    def set_type(self, type):
        self.mark_dirty()
        self.type = type


class StorageBackend:
    """Minimal superclass which just provides the (abstract) interface all backends must provide.

    Notably, backend is _NOT_ concerned with either in-block
    dependencies, or outside-the-tree references from inodes. backend
    just does what it does, i.o.w. mutate structures behind it on
    demand.
    """

    @property
    def block_id_key(self):
        return b''

    def delete_block(self, block):
        """Delete the block in storage.

        The block is StoredBlock and must exist."""
        raise NotImplementedError

    def flush_block(self, block):
        # 'block' was probably dirty', and should be flushed
        raise NotImplementedError

    def flush_done(self):
        pass

    def get_block_by_id(self, storage, block_id, *, cleanup=False):
        """Get data for the block identified by block_id.

        If the block does not exist, None is returned. If it exists,
        StoredBlock instance is returned.
        """
        raise NotImplementedError

    def get_block_id_by_name(self, n):
        """Get the block identifier for the named block.

        If the name is not set, None is returned.
        """
        raise NotImplementedError

    def get_bytes_available(self):
        raise NotImplementedError

    def get_bytes_used(self):
        raise NotImplementedError

    def set_block_name(self, block_id, n):
        """Set name of the block 'block_id' to 'n'.

        This does not change reference counts and therefore should be
        mostly used by the internal APIs.
        """
        raise NotImplementedError

    def store_block(self, block):
        """Store a StoredBlock block with the given block identifier.

        Note that it is an error to call this for already existing
        block.
        """
        raise NotImplementedError


class DictStorageBackend(StorageBackend):
    """ For testing purposes, in-memory dict-based storage backend. """

    def __init__(self):
        self.name2bid = {}
        self.bid2block = {}

    def delete_block(self, block):
        _debug('delete_block %s', block)
        del self.bid2block[block.id]

    def flush_block(self, block):
        # nop, it is already reflected in the block
        pass

    def get_block_by_id(self, storage, block_id, *, cleanup=False):
        return self.bid2block.get(block_id)

    def get_block_id_by_name(self, n):
        return self.name2bid.get(n)

    def get_bytes_available(self):
        return psutil.virtual_memory().available

    def get_bytes_used(self):
        seen = set()
        return getrecsizeof(self.name2bid, seen) + \
            getrecsizeof(self.bid2block, seen)

    def set_block_name(self, block_id, n):
        if block_id:
            self.name2bid[n] = block_id
        else:
            del self.name2bid[n]

    def store_block(self, block):
        assert block.id not in self.bid2block
        self.bid2block[block.id] = block


class SQLiteStorageBackend(StorageBackend):
    """SQLite storage backend.

    It does not probably perform THAT well but can be used to ensure
    things work correctly and as an added bonus has in-memory mode.

    TBD: using prdb code for this would have been 'nice' but I rather not
    mix the two hobby projects for now..
    """

    def __init__(self, *, codec=None, filename=':memory:', **kw):
        self.filename = filename
        self.codec = codec or NopBlockCodec()
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

    def get_block_by_id(self, storage, block_id, *, cleanup=False):
        _debug('get_block_by_id %s', block_id)
        r = self._get_execute_result(
            'SELECT data,refcnt,type FROM blocks WHERE id=?', (block_id,))
        if len(r) == 0:
            return
        assert len(r) == 1
        r = list(r[0])
        block_data, block_refcnt, block_type = r
        block_data = self.codec.decode_block(block_id, block_data)
        b = StoredBlock(storage, block_id, refcnt=block_refcnt, data=block_data,
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


class Storage:

    backend_class = None  # May be set by subclass

    _dirty_bid2block = None

    def __init__(self, *, backend=None):
        self.block_id_has_references_callbacks = []

        if backend is None:
            cl = self.backend_class
            assert cl
            backend = cl()
        assert isinstance(backend, StorageBackend)
        self.backend = backend
        # TBD: Are following 'proxy methods' sensible?
        for k in ['get_block_id_by_name',
                  'get_bytes_available', 'get_bytes_used', ]:
            if not hasattr(self, k):
                setattr(self, k, getattr(backend, k))
    referenced_refcnt0_block_ids = None

    def add_block_id_has_references_callback(self, callback):
        assert callback
        self.block_id_has_references_callbacks.append(callback)

    def block_data_references_callback(self, block_data):
        return _nopiterator

    def block_id_has_references_callback(self, block_id):
        return any(block_id
                   for cb in self.block_id_has_references_callbacks
                   if cb(block_id))

    @lazy_property
    def block_id_key(self):
        return self.backend.block_id_key

    def delete_block_id_if_no_extref(self, block_id):
        """This is the main delete function and should be called if the class
        cares about external dependencies. If external dependencies
        exist, this function will be eventually called by later flush
        methods until they do not exist.
        """
        _debug('delete_block_id_if_no_extref %s', block_id)
        if self.block_id_has_references_callback(block_id):
            if not self.referenced_refcnt0_block_ids:
                self.referenced_refcnt0_block_ids = set()
            self.referenced_refcnt0_block_ids.add(block_id)
            _debug(' .. externally referred, omitting for now')
            return
        if self.referenced_refcnt0_block_ids:
            self.referenced_refcnt0_block_ids.discard(block_id)
        return self.delete_block_id_with_deps(block_id)

    def delete_block_id_with_deps(self, block_id):
        """Delete block id and remove its references."""
        _debug('delete_block_id_with_deps %s', block_id)
        block = self.get_block_by_id(block_id, cleanup=True)
        assert block
        self.update_block_data_dependencies(block.data, False, block.type)
        self.delete_block_be(block)
        return True

    def delete_block_be(self, block):
        self.backend.delete_block(block)

    def flush(self):
        """ Attempt to get rid of dangling reference count 0 blocks. """
        deleted = True
        ops = 0
        while self.referenced_refcnt0_block_ids and deleted:
            s = self.referenced_refcnt0_block_ids
            del self.referenced_refcnt0_block_ids
            deleted = False
            for block_id in s:
                _debug('flush dangling %s', block_id)
                r = self.get_block_by_id(block_id, cleanup=True)
                if not r.refcnt:
                    if self.delete_block_id_if_no_extref(block_id):
                        ops += 1
                        deleted = True

        ops += self.flush_dirty_store_blocks()
        self.backend.flush_done()
        return ops

    def flush_block_be(self, block):
        return self.backend.flush_block(block)

    def flush_dirty_store_blocks(self):
        ops = 0
        while self._dirty_bid2block:
            blocks = self._dirty_bid2block.values()
            del self._dirty_bid2block
            for block in blocks:
                ops += self.flush_dirty_store_block(block)
                if not block.refcnt:
                    if self.delete_block_id_if_no_extref(block.id):
                        ops += 1
                        block.refcnt = None

        return ops

    def flush_dirty_store_block(self, block):
        return block.flush()

    def get_block_data_references(self, block_data):
        yield from self.block_data_references_callback(block_data)

    def get_block_by_id(self, block_id):
        d = self._dirty_bid2block
        if d:
            block = d.get(block_id)
            if block:
                return block
        return self.backend.get_block_by_id(self, block_id)

    def get_block_data_by_id(self, block_id):
        r = self.get_block_by_id(block_id)
        if r:
            return r.data

    def mark_block_dirty(self, block):
        if not self._dirty_bid2block:
            self._dirty_bid2block = {}
        self._dirty_bid2block[block.id] = block

    def refer_block(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r, 'block id %s awol' % block_id
        r.set_refcnt(r.refcnt + 1)

    def refer_or_store_block(self, block_id, block_data):
        """Convenience method for handling the common case of 'we have these
bytes, no clue if the block is already inserted (by someone else) ->
either refer to existing one, or add new block to the storage
layer."""
        if self.referenced_refcnt0_block_ids:
            self.referenced_refcnt0_block_ids.discard(block_id)
        r = self.get_block_by_id(block_id)
        if r is not None:
            self.refer_block(block_id)
        else:
            self.store_block(block_id, block_data)

    def release_block(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r
        refcnt = r.refcnt - 1
        assert refcnt >= 0
        r.set_refcnt(refcnt)
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
        self.set_block_name_be(block_id, n)

    def set_block_name_be(self, block_id, n):
        self.backend.set_block_name(block_id, n)

    def store_block(self, block_id, block_data, *, refcnt=1, type=const.BLOCK_TYPE_NORMAL):
        b = StoredBlock(self, block_id, data=block_data,
                        refcnt=refcnt, type=type)
        self.store_block_be(b)

    def store_block_be(self, block):
        self.backend.store_block(block)
        self.update_block_data_dependencies(block.data, True, block.type)

    def update_block_data_dependencies(self, block_data, is_add, block_type):
        if block_type >= const.BLOCK_TYPE_WANT_NORMAL:
            return
        bids = (x for x in self.get_block_data_references(block_data) if x)
        if is_add:
            for block_id in bids:
                self.refer_block(block_id)
        else:
            for block_id in bids:
                self.release_block(block_id)

    def updated_block_type(self, block, old_type, new_type):
        # Only case in which it is valid not to have data; all other state
        # transitions should end with us having data
        if new_type == const.BLOCK_TYPE_MISSING:
            assert old_type == const.BLOCK_TYPE_NORMAL
            return
        assert block.data is not None  # changing type on missing/want = bad
        self.update_block_data_dependencies(block.data, True, new_type)
        self.update_block_data_dependencies(block.data, False, old_type)


class DictStorage(Storage):
    backend_class = DictStorageBackend


class SQLiteStorage(Storage):
    backend_class = DictStorageBackend


class DelayedStorage(Storage):

    """In-memory storage handling; cache reads (up to a point) in addition
    to caching writes (better than) Storage. Storage will do immediate
    inserts, DelayedStorage will not. Otherwise the dirty handling is
    the same, though.
    """

    def __init__(self, **kw):
        Storage.__init__(self, **kw)
        assert isinstance(self.backend, StorageBackend)
        self._names = {}  # name -> current, orig

        self._cache_bid2block = {}

        self.maximum_cache_size = 0  # kept over flush()es
        self.cache_size = 0

    def _goc_block_by_id(self, block_id):
        if block_id not in self._cache_bid2block:
            r = Storage.get_block_by_id(self, block_id)
            if r:
                self.cache_size += util.getrecsizeof(r.data)
            else:
                r = StoredBlock(self, block_id)
            self._cache_bid2block[block_id] = r
        r = self._cache_bid2block[block_id]
        r.t = time.time()
        return r

    @property
    def calculated_cache_size(self):
        v = 0
        for b in self._cache_bid2block.values():
            if b.data:
                v += util.getrecsizeof(b.data)
        return v

    def _flush_names(self):
        ops = 0
        for block_name, o in self._names.items():
            (current_id, orig_id) = o
            if current_id != orig_id:
                self.backend.set_block_name(current_id, block_name)
                o[1] = o[0]
                ops += 1
        return ops

    def flush_dirty_store_block(self, block):
        self.cache_size += block.get_cache_delta()
        return block.flush()

    def _shrink_cache(self):
        l = list(self._cache_bid2block.values())
        goal = self.maximum_cache_size * 3 // 4
        _debug('_shrink_cache goal=%d < %d', goal, self.cache_size)
        # try to stay within [3/4 * max, max]
        l.sort(key=lambda k: k.t)  # last used time
        while l and self.cache_size > goal:
            block = l.pop(0)
            self._delete_cached_block(block)

    def _delete_cached_block(self, block):
        assert isinstance(block, StoredBlock)
        block2 = self._cache_bid2block.pop(block.id)
        assert block is block2
        self.cache_size -= block.cache_size
        if block.stored and block.stored.refcnt is None:
            # Locally stored, never hit disk; we have to get rid of
            # references if any.
            self.update_block_data_dependencies(block.data, False, block.type)

    def delete_block_be(self, block):
        Storage.delete_block_be(self, block)
        self._delete_cached_block(block)

    def flush(self):
        _debug('flush')
        ops = 0
        ops += self._flush_names()
        ops += Storage.flush(self)
        if self.maximum_cache_size and self.cache_size > self.maximum_cache_size:
            self._shrink_cache()
        return ops

    def flush_block_be(self, block):
        return self.backend.flush_block(block)

    def get_block_by_id(self, block_id, *, cleanup=False):
        _debug('%s.get_block_by_id %s', self.__class__.__name__, block_id)
        r = self._goc_block_by_id(block_id)
        _debug(' => %s', r)
        if not r.refcnt and not self.block_id_has_references_callback(block_id) and not cleanup:
            _debug(' [skip - no refcnt, not referred]')
            return
        return r

    def _get_block_id_by_name(self, n):
        if n not in self._names:
            block_id = self.backend.get_block_id_by_name(n)
            self._names[n] = [block_id, block_id]
        return self._names[n]

    def get_block_id_by_name(self, n):
        return self._get_block_id_by_name(n)[0]

    def set_block_name_be(self, block_id, n):
        self._get_block_id_by_name(n)[0] = block_id

    def store_block(self, block_id, block_data, *, refcnt=1, type=const.BLOCK_TYPE_NORMAL):
        _debug('store_block %s', block_id)
        assert isinstance(block_id, bytes)
        assert block_data
        block = self._goc_block_by_id(block_id)
        assert not block.refcnt and refcnt
        block.set_data(block_data)
        block.set_refcnt(refcnt)
        block.set_type(type)
        self.cache_size += block.get_cache_delta()
        self.update_block_data_dependencies(block.data, True, block.type)
