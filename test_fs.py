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
# Last modified: Tue Dec 13 20:14:35 2016 mstenber
# Edit time:     39 min
#
"""Tests that use actual real (mocked) filesystem using the llfuse ops
interface.

"""

import errno
import logging
import os

import pytest

import forest
import llfuse
import ops
from storage import NopBlockCodec, SQLiteStorage, TypedBlockCodec
from util import to_bytes

_debug = logging.getLogger(__name__).debug


class MockFile:

    def __init__(self, fs, fd, flags):
        self.fs = fs
        self.fd = fd
        self.ofs = 0
        self.flags = flags

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def close(self):
        if not self.fd:
            return
        self.fs.os_close(self.fd)
        self.fd = 0

    def fileno(self):
        return self.fd

    def flush(self):
        self.fs.ops.flush(self.fd)

    def read(self):
        raise NotImplementedError

    def seek(self, ofs):
        self.ofs = ofs

    def write(self, s):
        if not (self.flags & (os.O_RDWR | os.O_WRONLY)):
            raise IOError
        # TBD: O_APPEND = write always to the end
        pass


class MockFS:

    def __init__(self):
        storage = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
        f = forest.Forest(storage, llfuse.ROOT_INODE)
        self.ops = ops.Operations(f)
        self.rctx_root = llfuse.RequestContext()
        self.rctx_user = llfuse.RequestContext(uid=42, gid=7, pid=123)
        self.ops.init()

    def open(self, filename, mode):
        filename = to_bytes(filename)
        assert b'/' not in filename
        flags = 0
        char2flag = {'r': (os.O_RDONLY, 0),
                     'w': (os.O_WRONLY, 0),
                     '+': (os.O_RDWR, os.O_RDONLY | os.O_WRONLY),
                     'a': (os.O_WRONLY | os.O_APPEND, 0)}
        for char in mode:
            setbits, clearbits = char2flag[char]
            flags |= setbits
            flags &= ~clearbits
        try:
            _debug('attempting to lookup %s', filename)
            attrs = self.ops.lookup(llfuse.ROOT_INODE, filename,
                                    self.rctx_user)
            fd = self.ops.open(attrs.st_ino, flags, self.rctx_user)
            self.ops.forget([(attrs.st_ino, 1)])
        except llfuse.FUSEError as e:
            _debug('exception %s', repr(e))
            if not (flags & (os.O_WRONLY | os.O_RDWR)):
                raise IOError(errno.ENOENT)
            mode = 0
            cr_flags = 0
            fd, attrs = self.ops.create(llfuse.ROOT_INODE, filename,
                                        mode, cr_flags, self.rctx_user)
        return MockFile(self, fd, flags)

    def os_close(self, fd):
        assert isinstance(fd, int)
        self.ops.release(fd)

    def os_dup(self, fd):
        return self.ops.forest.lookup_fd(fd).dup()

    def os_listdir(self, path):
        assert path == '/'  # no support for subdirs, yet
        inode = llfuse.ROOT_INODE
        fd = self.ops.opendir(inode, self.rctx_user)
        l = []
        for n, a, i in self.ops.readdir(fd, 0):
            l.append(n.decode())
        self.ops.releasedir(fd)
        return l

    def os_unlink(self, path):
        path = to_bytes(path)
        assert b'/' not in path
        inode = llfuse.ROOT_INODE
        self.ops.unlink(inode, path, self.rctx_user)


def test_simple():
    mfs = MockFS()
    assert mfs.os_listdir('/') == []
    with mfs.open('file', 'w') as fh:
        fh.write('foo')
    assert mfs.os_listdir('/') == ['file']
    mfs.os_unlink('file')
    assert mfs.os_listdir('/') == []


#@pytest.mark.xfail(raises=llfuse.FUSEError, reason='pending code')
@pytest.mark.xfail(raises=NotImplementedError)
def test_unlink_behavior():
    """ This is based on the 'gotchas' in llfuse documentation. """
    mfs = MockFS()
    with mfs.open('file_one', 'w+') as fh1:
        fh1.write('foo')
        fh1.flush()
        assert 'file_one' in mfs.os_listdir('/')
        with mfs.open('file_one', 'a') as fh2:
            mfs.os_unlink('file_one')
            assert 'file_one' not in mfs.os_listdir('/')
            fh2.write('bar')
        mfs.os_close(mfs.os_dup(fh1.fileno()))
        fh1.seek(0)
        assert fh1.read() == 'foobar'
