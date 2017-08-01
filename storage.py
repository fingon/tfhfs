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
# Last modified: Tue Aug  1 18:37:22 2017 mstenber
# Edit time:     1024 min
#
"""This is the 'storage layer' main module.

It provides an abstract interface for the forest layer to use, and
uses storage backend for actual raw file operations.

TBD: how to handle maximum_cache_size related flushes? trigger timer
to flush earlier? within call stack, it is bad idea?

"""

import logging
import os
import time

import lz4.block
import psutil
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

import const
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
        cd = lz4.block.compress(d)
        if len(cd) < len(d):
            t = t | const.BIT_COMPRESSED
            d = cd
        return TypedBlockCodec.encode_block(self, block_id, (t, d))

    def decode_block(self, block_id, block_data):
        (t, d) = TypedBlockCodec.decode_block(self, block_id, block_data)
        if t & const.BIT_COMPRESSED:
            d = lz4.block.decompress(d)
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

    def __init__(self, parent, id, *, refcnt=None, data=None, type=None):
        assert isinstance(parent, Storage)
        self.parent = parent
        self.id = id
        # ^ should be immutable

        self.refcnt = refcnt
        self.data = data
        self.type = type
        # ^ should have accessors for mutation

    def __repr__(self):
        return '<%s %s %s refcnt:%s type:%s datalen:%s>' % (self.__class__.__name__, id(self),
                                                            self.id, self.refcnt,
                                                            self.type,
                                                            len(self.data or ''))

    @property
    def cache_size(self):
        return util.getrecsizeof(self.data) if self.data else 0

    def mark_dirty_related(self):
        assert self.stored is None
        self.stored = StoredBlock(self.parent, self.id, refcnt=self.refcnt,
                                  data=self.data, type=self.type)
        self.parent.mark_block_dirty(self)

    def perform_flush(self):
        assert isinstance(self.parent, Storage)
        ops = 0
        if self.stored.refcnt is None:
            if self.refcnt:
                _debug(' added to storage due to nonzero new refcnt')
                self.parent.backend.store_block(self)
                ops += 1
            else:
                # don't even remove self.stored - it retains the fact
                # this is not persisted to disk
                ops += self.parent.flush_block_be(self)
        else:
            _debug(' %s changed on disk', self)
            # It is stored on disk. Update it there.
            self.parent.updated_block_type(self, self.stored.type, self.type)
            ops += self.parent.flush_block_be(self)
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

    def get_block_by_id(self, storage, block_id):
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
        return 1

    def get_block_by_id(self, storage, block_id):
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


class Storage:

    backend_class = None  # May be set by subclass

    def __init__(self, *, backend=None):
        self.block_id_has_references_callbacks = []
        self._dirty_bid2block = {}
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
    referenced_refcnt0_blocks = None

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

    def delete_block_if_no_extref(self, block):
        """This is the main delete function and should be called if the class
        cares about external dependencies. If external dependencies
        exist, this function will be eventually called by later flush
        methods until they do not exist.
        """
        _debug('delete_block_if_no_extref %s', block)
        if self.block_id_has_references_callback(block.id):
            if not self.referenced_refcnt0_blocks:
                self.referenced_refcnt0_blocks = {}
            self.referenced_refcnt0_blocks[block.id] = block
            _debug(' .. externally referred, omitting for now')
            return
        if self.referenced_refcnt0_blocks:
            self.referenced_refcnt0_blocks.pop(block.id, None)
        return self.delete_block_with_deps(block)

    def delete_block_with_deps(self, block):
        """Delete block id and remove its references."""
        assert isinstance(block, StoredBlock)
        _debug('delete_block_with_deps %s', block)
        self.update_block_data_dependencies(block.data, False, block.type)
        self.delete_block_be(block)
        return True

    def delete_block_be(self, block):
        self.backend.delete_block(block)

    def flush(self):
        """ Attempt to get rid of dangling reference count 0 blocks. """
        _debug('Storage.flush')
        ops = 0
        while True:
            s = self.referenced_refcnt0_blocks
            if not s:
                break
            del self.referenced_refcnt0_blocks
            deleted = False
            for block in s.values():
                _debug('flush dangling %s', block)
                if not block.refcnt:
                    if self.delete_block_if_no_extref(block):
                        ops += 1
                        deleted = True
            if not deleted:
                break
        ops += self.flush_dirty_store_blocks()
        self.backend.flush_done()
        return ops

    def flush_block_be(self, block):
        return self.backend.flush_block(block)

    def flush_dirty_store_blocks(self):
        _debug('Storage.flush_dirty_store_blocks')
        ops = 0
        while self._dirty_bid2block:
            blocks = list(self._dirty_bid2block.values())
            self._dirty_bid2block.clear()
            nonzero_blocks = []
            # initially handle refcnt = 0 cases
            for block in blocks:
                assert block.dirty
                if not block.refcnt:
                    ops += block.flush()
                else:
                    nonzero_blocks.append(block)

            for block in nonzero_blocks:
                if block.refcnt:
                    if block.id not in self._dirty_bid2block:
                        ops += block.flush()
                else:
                    self._dirty_bid2block[block.id] = block
                    # populate for subsequent run
        return ops

    def get_block_data_references(self, block_data):
        yield from self.block_data_references_callback(block_data)

    def get_block_by_id(self, block_id):
        _debug('get_block_by_id %s', block_id)
        block = self._dirty_bid2block.get(block_id)
        if block and (block.refcnt or
                      self.block_id_has_references_callback(block_id)):
            _debug(' found in _dirty_bid2block: %s (%d refcnt)',
                   block, block.refcnt)
            return block
        d = self.referenced_refcnt0_blocks
        if d:
            block = d.get(block_id)
            if block and (block.refcnt or
                          self.block_id_has_references_callback(block_id)):
                _debug(' found in referenced_refcnt0_blocks: %s (%d refcnt)',
                       block, block.refcnt)
                return block
        _debug('falling back to storage')
        return self.backend.get_block_by_id(self, block_id)

    def get_block_data_by_id(self, block_id):
        r = self.get_block_by_id(block_id)
        if r:
            return r.data

    def mark_block_dirty(self, block):
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
        if not refcnt:
            if self.delete_block_if_no_extref(r):
                return False
        return True

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
        self.update_block_data_dependencies(b.data, True, b.type)
        self.backend.store_block(b)
        assert b.refcnt is not None

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
        # Only cases in which it is valid not to have data; all other state
        # transitions should end with us having data
        if new_type == const.BLOCK_TYPE_MISSING:
            assert old_type == const.BLOCK_TYPE_NORMAL
            return
        if new_type == const.BLOCK_TYPE_WEAK_MISSING:
            assert old_type == const.BLOCK_TYPE_WEAK
            return
        assert block.data is not None  # changing type on missing/want = bad
        self.update_block_data_dependencies(block.data, True, new_type)
        self.update_block_data_dependencies(block.data, False, old_type)


class DictStorage(Storage):
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
        r = self._cache_bid2block.get(block_id)
        if r is None:
            r = Storage.get_block_by_id(self, block_id)
            if r is None:
                r = StoredBlock(self, block_id)
                _debug('_goc_block_by_id added %s', r)
            else:
                _debug('_goc_block_by_id loaded %s', r)
            self.cache_size += r.cache_size
            self._cache_bid2block[block_id] = r
        r.t = time.time()
        return r

    @property
    def calculated_cache_size(self):
        v = 0
        for b in self._cache_bid2block.values():
            v += b.cache_size
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
        _debug('_delete_cached_block %s', block)
        assert isinstance(block, StoredBlock)
        block2 = self._cache_bid2block.pop(block.id)
        assert block is block2, '%s != %s' % (block, block2)
        self.cache_size -= block.cache_size
        if block.stored and block.stored.refcnt is None:
            # Locally stored, never hit disk; we have to get rid of
            # references if any (We add references when we add the object)
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
        if not block.refcnt:
            if block.stored.refcnt is None:
                self._delete_cached_block(block)
                return 0
            elif self.delete_block_if_no_extref(block):
                return 1
        return self.backend.flush_block(block)

    def get_block_by_id(self, block_id):
        _debug('%s.get_block_by_id %s', self.__class__.__name__, block_id)
        r = self._goc_block_by_id(block_id)
        _debug(' => %s', r)
        if not r.refcnt and not self.block_id_has_references_callback(block_id):
            _debug(' [skip - no refcnt, not referred]')
            return
        return r

    def _get_block_id_by_name(self, n):
        o = self._names.get(n)
        if o is not None:
            return o
        block_id = self.backend.get_block_id_by_name(n)
        self._names[n] = [block_id, block_id]
        return self._names[n]

    def get_block_id_by_name(self, n):
        return self._get_block_id_by_name(n)[0]

    def set_block_name_be(self, block_id, n):
        self._get_block_id_by_name(n)[0] = block_id

    def release_block(self, block_id):
        r = self.get_block_by_id(block_id)
        assert r
        refcnt = r.refcnt - 1
        assert refcnt >= 0
        r.set_refcnt(refcnt)
        if not refcnt:
            # actual deletion done later
            return False
        return True

    def store_block(self, block_id, block_data, *, refcnt=1, type=const.BLOCK_TYPE_NORMAL):
        _debug('store_block %s', block_id)
        assert isinstance(block_id, bytes)
        assert block_data
        block = self._goc_block_by_id(block_id)
        assert not block.refcnt and refcnt
        block.set_data(block_data)
        block.set_refcnt(refcnt)
        block.set_type(type)
        assert block.stored.refcnt is None
        self.cache_size += block.cache_size
        self.update_block_data_dependencies(block.data, True, block.type)
