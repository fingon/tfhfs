#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_fs.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Dec 10 20:32:55 2016 mstenber
# Last modified: Sat Dec 10 20:40:34 2016 mstenber
# Edit time:     5 min
#
"""Tests that use actual real (mocked) filesystem using the llfuse ops
interface.

"""

import pytest


class MockFS:

    def open(self, filename, mode):
        raise NotImplementedError

    def os_close(self, fd):
        assert isinstance(fd, int)
        raise NotImplementedError

    def os_dup(self, fd):
        assert isinstance(fd, int)
        raise NotImplementedError

    def os_listdir(self, path):
        raise NotImplementedError

    def os_unlink(self, path):
        raise NotImplementedError


@pytest.mark.xfail(raises=NotImplementedError)
def test_unlink_behavior():
    """ This is based on the 'gotchas' in llfuse documentation. """
    mfs = MockFS()
    with mfs.open('mnt/file_one', 'w+') as fh1:
        fh1.write('foo')
        fh1.flush()
        with mfs.open('mnt/file_one', 'a') as fh2:
            mfs.os_unlink('mnt/file_one')
            assert 'file_one' not in mfs.os_listdir('mnt')
            fh2.write('bar')
        mfs.os_close(mfs.os_dup(fh1.fileno()))
        fh1.seek(0)
        assert fh1.read() == 'foobar'
