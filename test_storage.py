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
# Last modified: Thu Jun 30 15:28:29 2016 mstenber
# Edit time:     32 min
#
"""

"""

from storage import SQLiteStorage, DelayedStorage, ConfidentialBlockCodec, TypedBlockCodec, NopBlockCodec, CompressingTypedBlockCodec
import pytest
import sqlite3
import cryptography.exceptions
import unittest


def _nop():
    pass


def _prod_storage(s, flush=_nop):
    # refcnt = 1
    s.store_block(b'foo', b'bar')
    flush()
    assert s.get_block_data_by_id(b'foo') == b'bar'

    # refcnt = 2
    s.refer_block(b'foo')
    flush()
    assert s.get_block_data_by_id(b'foo') == b'bar'

    # refcnt = 1
    assert s.release_block(b'foo')
    flush()
    assert s.get_block_data_by_id(b'foo') == b'bar'

    # refcnt = 0 => should be gone
    assert not s.release_block(b'foo')
    flush()
    assert s.get_block_data_by_id(b'foo') == None

    assert s.get_block_id_by_name(b'foo') == None
    s.set_block_name(b'bar', b'foo')
    flush()
    assert s.get_block_id_by_name(b'foo') == b'bar'

    s.set_block_name(None, b'foo')
    flush()
    assert s.get_block_id_by_name(b'foo') == None

    # Assume it did not have dependency handling before; add it now:
    deps = {b'content1': [b'id2']}

    def _depfun(block_id):
        return deps.get(block_id, [])
    s.block_data_references_callback = _depfun
    s.store_block(b'id2', b'content2')
    s.store_block(b'id1', b'content1')
    flush()
    assert s.get_block_by_id(b'id2')[1] == 2
    assert s.get_block_by_id(b'id1')[1] == 1
    s.release_block(b'id1')
    flush()
    assert s.get_block_by_id(b'id2')[1] == 1
    s.release_block(b'id2')
    flush()


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


@pytest.mark.xfail(raises=sqlite3.OperationalError)
def test_sqlitestorage_error():
    s = SQLiteStorage()
    s._get_execute_result('BLORB')


def test_sqlitestorage_error_ignored():
    s = SQLiteStorage()
    s._get_execute_result('BLORB', ignore_errors=True)


def test_delayedstorage_immediate_flush():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    _prod_storage(s, s.flush)


def test_delayedstorage_immediate_flush_no_cache():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    s.maximum_cache_size = 1
    _prod_storage(s, s.flush)


def test_delayedstorage_immediate_flush_no_dirty():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    s.maximum_dirty_size = 1
    _prod_storage(s, s.flush)


def test_delayedstorage_no_dirty():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    s.maximum_dirty_size = 1
    _prod_storage(s)


def test_delayedstorage():
    s2 = SQLiteStorage()
    s = DelayedStorage(s2)
    _prod_storage(s)

    # Should be nop as we deleted everything we added
    assert not s.flush()
    assert not s.cache_size
    assert not s.dirty_size

    # refcnt = 1
    s.store_block(b'foo', b'bar')
    assert s.get_block_data_by_id(b'foo') == b'bar'
    assert not s2.get_block_data_by_id(b'foo') == b'bar'
    assert not s.cache_size
    assert s.dirty_size

    assert s.flush() == 1
    assert s2.get_block_data_by_id(b'foo') == b'bar'
    assert s.cache_size
    assert not s.dirty_size

    assert s._blocks
    s.flush()
    assert s._blocks

    s.maximum_cache_size = 1
    s.flush()
    assert not s._blocks
