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
# Last modified: Wed Dec 14 09:13:38 2016 mstenber
# Edit time:     130 min
#
"""

"""

import logging
import sqlite3
import unittest

import cryptography.exceptions
import pytest

from storage import (CompressingTypedBlockCodec, ConfidentialBlockCodec,
                     DelayedStorage, DictStorage, NopBlockCodec, SQLiteStorage,
                     TypedBlockCodec)

_debug = logging.getLogger(__name__).debug


def _nop():
    pass


def _flush_twice(s, flush):
    if isinstance(s, DelayedStorage):
        assert s.cache_size == s.calculated_cache_size, 'pre-cache'
        assert s.dirty_size >= s.calculated_dirty_size, 'pre-dirty'
    if flush():
        assert not flush()
        if isinstance(s, DelayedStorage):
            assert s.cache_size == s.calculated_cache_size, 'post-cache'
            assert s.dirty_size >= s.calculated_dirty_size, 'post-dirty'


def _prod_storage(s, flush=_nop):
    # By default we retain the block with idk id ('keep')
    if flush is not _nop:
        s.set_block_id_has_references_callback(lambda x: x == b'idk')

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
    deps = {b'content1': [b'id2']}

    def _depfun(block_id):
        return deps.get(block_id, [])
    _debug('## add id2 + id1 with dep on id2')
    s.block_data_references_callback = _depfun
    s.store_block(b'id2', b'content2')
    s.store_block(b'id1', b'content1')
    _flush_twice(s, flush)
    assert s.get_block_by_id(b'id2')[1] == 2
    assert s.get_block_by_id(b'id1')[1] == 1

    _debug('## release id2 (should still be around due to dep in id1)')
    s.release_block(b'id2')
    _flush_twice(s, flush)
    assert s.get_block_by_id(b'id2')[1] == 1
    assert s.get_block_by_id(b'id1')[1] == 1

    _debug('## release id1 (should release both)')
    s.release_block(b'id1')
    _flush_twice(s, flush)
    assert not s.get_block_by_id(b'id1')
    assert not s.get_block_by_id(b'id2')

    # Add fictional block which has external references
    if flush is not _nop:
        s.store_block(b'idk', b'contentk')
        _flush_twice(s, flush)
        s.release_block(b'idk')
        _flush_twice(s, flush)
        assert s.get_block_by_id(b'idk')[1] == 0

        # Back to class default
        s.set_block_id_has_references_callback(None)
        _flush_twice(s, flush)
        assert not s.get_block_by_id(b'idk')

    _debug('_prod_storage done')


def _prod_delayedstorage(s, s2, flush=_nop):
    # this isn't really pretty but oh well..
    # _prod_storage uses the initial callback, and so should we, although
    # DelayedStorage should not really care about the callback to start with
    assert isinstance(s, DelayedStorage)
    assert not isinstance(s2, DelayedStorage)
    _prod_storage(s, flush=flush)
    # Should be nop as we deleted everything we added
    if s.maximum_dirty_size:
        s.flush()  # may have pending subsequent changes
    else:
        # everything from _prod_storage should be in dirty cache => stateless
        assert not s.flush()

    # ensure repeat set_block_name is nop
    s.set_block_name(None, 'foo')
    assert not s.flush()

    assert not s.cache_size, s._blocks
    assert not s.dirty_size

    # refcnt = 1
    s.store_block(b'foo', b'bar')
    assert s.get_block_data_by_id(b'foo') == b'bar'
    if not s.maximum_dirty_size:
        assert s2.get_block_data_by_id(b'foo') == None
        assert s.cache_size
        assert s.dirty_size

        assert s.flush() == 1
    assert s2.get_block_data_by_id(b'foo') == b'bar'
    if not s.maximum_cache_size:
        assert s.cache_size
        assert s._blocks
    assert not s.dirty_size

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
        self.cbc = ConfidentialBlockCodec(self.password)
        self.ciphertext = self.cbc.encode_block(self.block_id, self.plaintext)

    def test_confidentialblockcodec(self):
        plaintext = self.cbc.decode_block(self.block_id, self.ciphertext)
        assert plaintext == self.plaintext

    @pytest.mark.xfail(raises=cryptography.exceptions.InvalidTag)
    def test_confidentialblockcodec_decode_error_1(self):
        self.cbc.decode_block(self.block_id + b'42', self.ciphertext)

    @pytest.mark.xfail(raises=cryptography.exceptions.InvalidTag)
    def test_confidentialblockcodec_decode_error_1(self):
        s = self.ciphertext + b'42'  # 'flawed' input -> cannot pass check
        self.cbc.decode_block(self.block_id, s)

    @pytest.mark.xfail(raises=AssertionError)
    def test_confidentialblockcodec_decode_error_2(self):
        self.cbc.decode_block(self.block_id, b'x')


def test_typeencoding():
    t = TypedBlockCodec(NopBlockCodec())
    s = t.encode_block(None, (7, b'42'))
    assert s == bytes([7]) + b'42'
    assert t.decode_block(None, s) == (7, b'42')


def test_compression():
    c = CompressingTypedBlockCodec(NopBlockCodec())
    plaintext = b'1234567890' * 50
    s = c.encode_block(None, (7, plaintext))
    assert len(s) < len(plaintext)  # woah, miracle of compression happened!
    assert c.decode_block(None, s) == (7, plaintext)


def test_compression_fail():
    c = CompressingTypedBlockCodec(NopBlockCodec())
    plaintext = b'1'
    s = c.encode_block(None, (7, plaintext))
    assert len(s) == (1 + len(plaintext))  # no compression :p
    assert c.decode_block(None, s) == (7, plaintext)


def test_sqlitestorage():
    s = SQLiteStorage()
    _prod_storage(s)


@pytest.mark.parametrize('kwargs', [
    pytest.mark.xfail({}, raises=sqlite3.OperationalError),
    dict(ignore_errors=True),
])
def test_sqlitestorage_get_execute_error(kwargs):
    s = SQLiteStorage()
    s._get_execute_result('BLORB', **kwargs)


@pytest.mark.parametrize('storage_attrs, use_flush, backend',
                         [
                             ({}, True, SQLiteStorage),
                             ({}, True, DictStorage),
                             ({'maximum_cache_size': 1}, True,
                              DictStorage),  # no cache
                             ({'maximum_dirty_size': 1},  True,
                              DictStorage
                              ),  # no dirty
                             ({}, False, DictStorage),
                         ])
def test_delayedstorage(storage_attrs, use_flush, backend):
    s2 = backend()
    s = DelayedStorage(s2)
    for k, v in storage_attrs.items():
        setattr(s, k, v)
    _prod_delayedstorage(s, s2, use_flush and s.flush or _nop)
