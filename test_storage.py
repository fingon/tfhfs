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
# Last modified: Wed Jun 29 10:44:05 2016 mstenber
# Edit time:     1 min
#
"""

"""

from storage import SQLiteStorage


def test_sqlitestorage():
    s = SQLiteStorage()

    # refcnt = 1
    s.store_block(b'foo', b'bar')
    assert s.get_block_by_id(b'foo') == b'bar'
    # refcnt = 2
    s.store_block(b'foo')
    assert s.get_block_by_id(b'foo') == b'bar'

    # refcnt = 1
    s.release_block(b'foo')
    assert s.get_block_by_id(b'foo') == b'bar'

    # refcnt = 0 => should be gone
    s.release_block(b'foo')
    assert s.get_block_by_id(b'foo') == None

    assert s.get_block_id_by_name(b'foo') == None
    s.set_block_id_name(b'bar', b'foo')
    assert s.get_block_id_by_name(b'foo') == b'bar'
