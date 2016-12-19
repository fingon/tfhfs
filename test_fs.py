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
# Last modified: Mon Dec 19 17:40:51 2016 mstenber
# Edit time:     146 min
#
"""Tests that use actual real (mocked) filesystem using the llfuse ops
interface.

"""

import contextlib
import errno
import logging
import os

import pytest

import const
import forest
import llfuse
import ops
import storage as st
from util import to_bytes

_debug = logging.getLogger(__name__).debug

O_BINARY = os.O_DIRECTORY  # reuse :p


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

    @property
    def inode(self):
        return self.fs.forest.lookup_fd(self.fd).inode

    def read(self, count=const.BLOCK_SIZE_LIMIT * 123):
        r = self.fs.ops.read(self.fd, self.ofs, count)
        self.ofs += len(r)
        if not (self.flags & O_BINARY):
            r = r.decode()
        return r

    def seek(self, ofs):
        self.ofs = ofs

    def write(self, s):
        if not (self.flags & O_BINARY):
            s = to_bytes(s)
        if not (self.flags & (os.O_RDWR | os.O_WRONLY)):
            raise IOError
        r = self.fs.ops.write(self.fd, self.ofs, s)
        self.ofs += r
        return r


class MockFS:

    def __init__(self, *, storage=None):
        storage = storage or st.DictStorage()
        self.forest = forest.Forest(storage, llfuse.ROOT_INODE)
        self.ops = ops.Operations(self.forest)
        self.rctx_root = llfuse.RequestContext()
        self.rctx_user = llfuse.RequestContext(uid=42, gid=7, pid=123)
        self.ops.init()

    def open(self, filename, mode):
        filename = to_bytes(filename)
        assert b'/' not in filename
        flags = 0
        char2flag = {'r': (os.O_RDONLY, 0),
                     'w': (os.O_WRONLY | os.O_TRUNC | os.O_CREAT, 0),
                     '+': (os.O_RDWR, os.O_RDONLY | os.O_WRONLY),
                     'a': (os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0),
                     'b': (O_BINARY, 0)}
        for char in mode:
            setbits, clearbits = char2flag[char]
            flags |= setbits
            flags &= ~clearbits
        fd = None
        try:
            _debug('attempting to lookup %s', filename)
            attrs = self.ops.lookup(llfuse.ROOT_INODE, filename,
                                    self.rctx_user)
            if flags & (os.O_CREAT | os.O_EXCL) == (os.O_CREAT | os.O_EXCL):
                if attrs:
                    self.ops.forget1(attrs.st_ino)
                    raise IOError
            else:
                fd = self.ops.open(attrs.st_ino, flags, self.rctx_user)
                self.ops.forget1(attrs.st_ino)
        except llfuse.FUSEError as e:
            _debug('exception %s', repr(e))
        if fd is None:
            if not (flags & os.O_CREAT):
                raise IOError(errno.ENOENT)
            if not (flags & (os.O_WRONLY | os.O_RDWR)):
                raise IOError(errno.ENOENT)
            mode = 0o600
            fd, attrs = self.ops.create(llfuse.ROOT_INODE, filename,
                                        mode, flags, self.rctx_user)
            assert attrs.st_ino
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
        ofs = 0
        while True:
            try:
                n, a, ofs = next(self.ops.readdir(fd, ofs))
                assert a.st_ino  # otherwise not visible in ls
            except StopIteration:
                break
            l.append(n.decode())
        self.ops.releasedir(fd)
        return l

    def os_stat(self, path):
        if path == '/':
            path = b'.'
        else:
            assert '/' not in path
            path = to_bytes(path)
        inode = llfuse.ROOT_INODE
        try:
            attrs = self.ops.lookup(inode, path, self.rctx_user)
        except llfuse.FUSEError:
            raise FileNotFoundError
        self.ops.forget1(attrs.st_ino)
        return attrs

    def os_unlink(self, path):
        path = to_bytes(path)
        assert b'/' not in path
        inode = llfuse.ROOT_INODE
        self.ops.unlink(inode, path, self.rctx_user)


def test_os_stat():
    mfs = MockFS()
    a = mfs.os_stat('/')
    assert a.st_ino == llfuse.ROOT_INODE
    assert mfs.os_listdir('/') == []  # should not be visible
    with contextlib.suppress(FileNotFoundError):
        a = mfs.os_stat('file')
        assert False
    assert a.st_ino
    with mfs.open('file', 'w') as fh:
        fh.write('foo')
    a = mfs.os_stat('file')
    assert a.st_ino
    assert mfs.os_listdir('/') == ['file']


@pytest.mark.timeout(2)
@pytest.mark.parametrize('modesuffix,content,count', [
    ('', 'foo', 1),
    ('b', b'foo', 1),
    ('', '1', const.INTERNED_BLOCK_DATA_SIZE_LIMIT + 1),
    ('b', b'2', const.INTERNED_BLOCK_DATA_SIZE_LIMIT + 2),
    ('', '3', const.BLOCK_SIZE_LIMIT + 3),
    ('b', b'4', 3 * const.BLOCK_SIZE_LIMIT + 4),
])
def test_file_content(modesuffix, content, count):
    content = content * count
    mfs = MockFS()
    # Ensure empty instance is empty
    assert mfs.os_listdir('/') == []
    # And we can write file
    with mfs.open('file', 'w' + modesuffix) as fh:
        fh.write(content)
    # And read it back
    _debug('read file')
    assert mfs.os_listdir('/') == ['file']
    with mfs.open('file', 'r' + modesuffix) as fh:
        got = fh.read()
        assert len(got) == len(content)
        assert got == content
        assert fh.inode.size == len(content)

    mfs.forest.flush()

    # And read it back from storage too
    mfs2 = MockFS(storage=mfs.forest.storage)
    assert mfs2.os_listdir('/') == ['file']
    _debug('read file (second storage)')
    with mfs2.open('file', 'r' + modesuffix) as fh:
        got = fh.read()
        assert len(got) == len(content)
        assert got == content
        assert fh.inode.size == len(content)

    # And remove it
    mfs.os_unlink('file')
    assert mfs.os_listdir('/') == []


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


def test_huge_file():
    """ Test that a HUGE(tm) file reads out all zeroes (and this will not end in tears) """
    hugefilesize = 1e12 + 42  # 1 terabyte
    middlish = hugefilesize // 2 + 13
    mfs = MockFS()
    with mfs.open('file', 'wb') as f:
        f.write(b'a')
        f.seek(middlish)
        f.write(b'b')
        f.seek(hugefilesize)
        f.write(b'c')
    with mfs.open('file', 'rb') as f:
        assert f.inode.size == hugefilesize + 1
        cnt = 1000
        assert f.read(cnt) == b'a' + bytes([0] * (cnt - 1))
        f.seek(middlish)
        assert f.read(cnt) == b'b' + bytes([0] * (cnt - 1))
        f.seek(hugefilesize)
        assert f.read() == b'c'


if __name__ == '__main__':
    # TBD - argument parsing?
    import argparse
    p = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('--cache-size', type=int, default=1024**3,
                   help='Maximum cache size used by storage')
    p.add_argument('--compress', '-c', action='store_true',
                   help='Compress the content opportunistically')
    p.add_argument('--dirty-size', type=int, default=1024**2,
                   help='Maximum dirty size used by storage')
    p.add_argument('--debug', '-d', action='store_true',
                   help='Enable debugging')
    p.add_argument(
        '--filename', '-f',
        help='Filename to store the data in')
    p.add_argument('--mountpoint', '-m',
                   default='/tmp/x',
                   help='Where the file should be mounted')
    p.add_argument('--salt', help='Salt to use')
    p.add_argument(
        '--password', '-p',
        help='Program to get the password from for encryption')
    args = p.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    if args.filename:
        if args.password:
            password = os.popen(args.password, 'r').read().strip().encode()
            salt = args.salt and args.salt.encode() or b''
            codec = st.ConfidentialBlockCodec(password, salt)
        else:
            codec = st.NopBlockCodec()
        if args.compress:
            codec = st.CompressingTypedBlockCodec(codec)
        else:
            codec = st.TypedBlockCodec(codec)
        storage = st.DelayedStorage(st.SQLiteStorage(codec=codec,
                                                     filename=args.filename))
        storage.maximum_cache_size = args.cache_size
        storage.maximum_dirty_size = args.dirty_size

    else:
        storage = st.DictStorage()
    forest = forest.Forest(storage, llfuse.ROOT_INODE)
    ops = ops.Operations(forest)
    fuse_options = set(llfuse.default_options)
    fuse_options.remove('nonempty')  # TBD..
    fuse_options.add('fsname=test_fs')
    if args.debug:
        fuse_options.add('debug')
    llfuse.init(ops, args.mountpoint, fuse_options)
    try:
        llfuse.main(workers=1)
    except:
        llfuse.close(unmount=False)
        raise
    llfuse.close()
