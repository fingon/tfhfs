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
# Last modified: Sat Dec 24 07:16:34 2016 mstenber
# Edit time:     176 min
#
"""

"""

import logging
import os.path
import sqlite3
import tempfile
import unittest

import cryptography.exceptions
import pytest

import storage as st

_debug = logging.getLogger(__name__).debug


def _nop():
    pass


def _flush_twice(s, flush):
    if isinstance(s, st.DelayedStorage):
        assert s.cache_size == s.calculated_cache_size, 'pre-cache'
    if flush():
        assert not flush()
        if isinstance(s, st.DelayedStorage):
            assert s.cache_size == s.calculated_cache_size, 'post-cache'


def _prod_storage(s, flush=_nop):
    assert isinstance(s, st.Storage)
    # By default we retain the block with idk id ('keep')
    if flush is not _nop:
        s.add_block_id_has_references_callback(lambda x: x == b'idk')

    _debug('## initial foo=bar')
    # refcnt = 1
    s.store_block(b'foo', b'bar')
    _flush_twice(s, flush)
    assert s.get_block_data_by_id(b'foo') == b'bar'

    # refcnt = 2
    s.refer_block(b'foo')
    _flush_twice(s, flush)
    assert s.get_block_data_by_id(b'foo') == b'bar'

    # refcnt = 1
    assert s.release_block(b'foo')
    _flush_twice(s, flush)
    assert s.get_block_data_by_id(b'foo') == b'bar'

    # refcnt = 0 => should be gone
    _debug('## final release of foo')
    assert not s.release_block(b'foo')
    _flush_twice(s, flush)
    _debug('# foo should be gone')
    assert s.get_block_data_by_id(b'foo') == None

    # precond - 'bar' should not exist
    assert s.get_block_data_by_id(b'bar') == None
    _debug('## initial bar=data')
    s.store_block(b'bar', b'data')
    _flush_twice(s, flush)
    assert s.get_block_id_by_name(b'foo') == None
    assert s.get_block_data_by_id(b'bar') == b'data'

    _debug('## label of bar => foo')
    s.set_block_name(b'bar', b'foo')
    _flush_twice(s, flush)

    _debug('## release bar')
    s.release_block(b'bar')
    _flush_twice(s, flush)
    assert s.get_block_id_by_name(b'foo') == b'bar'
    assert s.get_block_data_by_id(b'bar') == b'data'

    _debug('## label foo = None')
    s.set_block_name(None, b'foo')
    _flush_twice(s, flush)
    assert s.get_block_id_by_name(b'foo') == None
    assert s.get_block_data_by_id(b'bar') == None

    # Assume it did not have dependency handling before; add it now:
    deps = {b'content1': [b'id2'],
            b'contentk': [b'idk2']}

    def _depfun(block_data):
        d = deps.get(block_data, [])
        _debug('_depfun %s = %s', block_data, d)
        return d
    _debug('## add id2 + id1 with dep on id2')
    s.set_block_data_references_callback(_depfun)
    s.store_block(b'id2', b'content2')
    s.store_block(b'id1', b'content1')
    _flush_twice(s, flush)
    assert s.get_block_by_id(b'id1')[1] == 1
    if flush is not _nop:
        assert s.get_block_by_id(b'id2')[1] == 2

    _debug('## release id2 (should still be around due to dep in id1)')
    s.release_block(b'id2')
    _flush_twice(s, flush)
    assert s.get_block_by_id(b'id2')[1] == 1
    assert s.get_block_by_id(b'id1')[1] == 1

    _debug('## release id1 (should release both)')
    s.release_block(b'id1')
    _flush_twice(s, flush)
    assert not s.get_block_by_id(b'id1')
    if flush is not _nop:
        # Flush takes care of getting rid of id2
        assert not s.get_block_by_id(b'id2')

        # Add fictional block which has external references and depends on
        # another one
        _debug('## idk+idk2, referred from inode')
        s.store_block(b'idk2', b'contentk2')
        s.store_block(b'idk', b'contentk')
        _flush_twice(s, flush)
        assert s.get_block_by_id(b'idk2')[1] == 2
        assert s.get_block_by_id(b'idk')[1] == 1
        s.release_block(b'idk2')
        s.release_block(b'idk')
        _debug('# released; should still have post-flush')
        _flush_twice(s, flush)
        assert s.get_block_by_id(b'idk2')[1] == 1
        assert s.get_block_by_id(b'idk')[1] == 0
        assert s.referenced_refcnt0_block_ids
        # Back to class default
        _debug('# no longer referred by inode')
        s.block_id_has_references_callbacks = []  # for testing purposes only
        _flush_twice(s, flush)
        assert not s.referenced_refcnt0_block_ids
        assert not s.get_block_by_id(b'idk')
        assert not s.get_block_by_id(b'idk2')

    _debug('_prod_storage done')


def _prod_delayedstorage(s, be, flush=_nop):
    # this isn't really pretty but oh well..
    # _prod_storage uses the initial callback, and so should we, although
    # DelayedStorage should not really care about the callback to start with
    assert isinstance(s, st.DelayedStorage)
    assert not isinstance(be, st.DelayedStorage)
    _prod_storage(s, flush=flush)
    # everything from _prod_storage should be in dirty cache => stateless
    assert not s.flush()

    # ensure repeat set_block_name is nop
    s.set_block_name(None, 'foo')
    assert not s.flush()

    assert not s.cache_size, s._blocks

    # refcnt = 1
    s.store_block(b'foo', b'bar')
    assert s.get_block_data_by_id(b'foo') == b'bar'
    assert be.get_block_by_id(b'foo') == None
    assert s.cache_size

    assert s.flush() == 1
    assert be.get_block_by_id(b'foo')[0] == b'bar'
    if not s.maximum_cache_size:
        assert s.cache_size
        assert s._blocks

    _flush_twice(s, flush)
    if not s.maximum_cache_size:
        assert s._blocks

    s.maximum_cache_size = -1  # may leave some metadata, but no block_data
    s.flush()
    assert not s.cache_size, s._blocks
    assert not s.calculated_cache_size


class ConfidentialBlockCodecTests(unittest.TestCase):

    def setUp(self):
        self.password = b'assword'
        self.plaintext = b'foo'
        self.block_id = b'12345678901234567890123456789012'
        self.cbc = st.ConfidentialBlockCodec(self.password)
        self.ciphertext = self.cbc.encode_block(self.block_id, self.plaintext)

    def test_confidential_blockcodec(self):
        plaintext = self.cbc.decode_block(self.block_id, self.ciphertext)
        assert plaintext == self.plaintext

    def test_confidential_blockcodec_decode_error_block_id_len(self):
        with pytest.raises(AssertionError):
            self.cbc.decode_block(self.block_id + b'42', self.ciphertext)

    def test_confidential_blockcodec_decode_error_block_id_value(self):
        with pytest.raises(cryptography.exceptions.InvalidTag):
            self.cbc.decode_block(self.block_id[:-2] + b'42', self.ciphertext)

    def test_confidential_blockcodec_decode_error_block_data(self):
        s = self.ciphertext + b'42'  # 'flawed' input -> cannot pass check
        with pytest.raises(cryptography.exceptions.InvalidTag):
            self.cbc.decode_block(self.block_id, s)

    def test_confidential_blockcodec_decode_error_garbage(self):
        with pytest.raises(AssertionError):
            self.cbc.decode_block(self.block_id, b'x')


def test_typeencoding():
    t = st.TypedBlockCodec(st.NopBlockCodec())
    s = t.encode_block(None, (7, b'42'))
    assert s == bytes([7]) + b'42'
    assert t.decode_block(None, s) == (7, b'42')


def test_compression():
    c = st.CompressingTypedBlockCodec(st.NopBlockCodec())
    plaintext = b'1234567890' * 50
    s = c.encode_block(None, (7, plaintext))
    assert len(s) < len(plaintext)  # woah, miracle of compression happened!
    assert c.decode_block(None, s) == (7, plaintext)


def test_compression_fail():
    c = st.CompressingTypedBlockCodec(st.NopBlockCodec())
    plaintext = b'1'
    s = c.encode_block(None, (7, plaintext))
    assert len(s) == (1 + len(plaintext))  # no compression :p
    assert c.decode_block(None, s) == (7, plaintext)


def test_storage_backends(backend):
    _prod_storage(st.Storage(backend=backend))


def test_storage_wrapped(storage):
    _prod_storage(storage)


@pytest.mark.parametrize('kwargs', [
    pytest.mark.xfail({}, raises=sqlite3.OperationalError),
    dict(ignore_errors=True),
])
def test_sqlitestorage_get_execute_error(kwargs):
    s = st.SQLiteStorageBackend()
    s._get_execute_result('BLORB', **kwargs)


def test_sqlitestorage_available_file():
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, 'x.db')
        s = st.SQLiteStorageBackend(filename=path)
        assert s.get_bytes_available()


_backends = {'sqlite': st.SQLiteStorageBackend, 'dict': st.DictStorageBackend}


@pytest.fixture(params=['sqlite', 'dict'])
def backend(request):
    return _backends[request.param]()

_storages = {'sqlite': st.SQLiteStorage, 'dict': st.DictStorage}


@pytest.fixture(params=['sqlite', 'dict'])
def storage(request):
    return _storages[request.param]()


@pytest.mark.parametrize('storage_attrs, use_flush',
                         [
                             ({}, True),
                             ({}, True),
                             ({'maximum_cache_size': 1}, True),  # no cache
                             ({}, False),
                         ])
def test_delayedstorage(storage_attrs, use_flush, backend):
    s = st.DelayedStorage(backend=backend)
    for k, v in storage_attrs.items():
        setattr(s, k, v)
    _prod_delayedstorage(s, backend, use_flush and s.flush or _nop)
    assert s.get_bytes_available() > 0
    assert s.get_bytes_used() > 0
