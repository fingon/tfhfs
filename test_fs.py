#!/usr/bin/env python3
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
# Last modified: Fri Dec 30 13:07:03 2016 mstenber
# Edit time:     209 min
#
"""Tests that use actual real (mocked) filesystem using the llfuse ops
interface.

"""

import argparse
import contextlib
import errno
import itertools
import logging
import os
from ms.lazy import lazy_property
import pytest

import const
import forest
import llfuse
import ops
import storage as st
from test_ops import SetattrFieldsIsh
from util import to_bytes, zeropad_bytes

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
        return self.fs.forest.fds.get_by_value(self.fd).inode

    def read(self, count=const.BLOCK_SIZE_LIMIT * 123):
        r = self.fs.ops.read(self.fd, self.ofs, count)
        self.ofs += len(r)
        if not (self.flags & O_BINARY):
            r = r.decode()
        return r

    def seek(self, ofs):
        self.ofs = ofs

    def truncate(self, pos=None):
        if pos is None:
            pos = self.tell()
        a = llfuse.EntryAttributes()
        a.st_size = pos
        f = SetattrFieldsIsh()
        f.update_size = True
        self.fs.ops.setattr(self.inode.value, a, f, self.fd,
                            self.fs.rctx_user)

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
        self.forest = forest.Forest(storage)
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
        assert fd
        return MockFile(self, fd, flags)

    def os_close(self, fd):
        assert isinstance(fd, int)
        self.ops.release(fd)

    def os_dup(self, fd):
        return self.ops.forest.fds.get_by_value(fd).dup()

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


@pytest.mark.parametrize('order', list(itertools.permutations((0, 1, 2))))
def test_huge_file(order):
    """ Test that a HUGE(tm) file reads out all zeroes (and this will not end in tears) """
    hugefilesize = int(1e18) + 42  # 1 exabyte
    middlish = hugefilesize // 2 + 13
    mfs = MockFS()
    with mfs.open('file', 'wb') as f:
        for op in order:
            if op == 0:
                f.seek(0)
                f.write(b'a')
            elif op == 1:
                f.seek(middlish)
                f.write(b'b')
            elif op == 2:
                f.seek(hugefilesize)
                f.write(b'c')
    with mfs.open('file', 'rb') as f:
        assert f.inode.size == hugefilesize + 1
        cnt = 1000
        assert f.read(cnt) == zeropad_bytes(cnt, b'a')
        f.seek(middlish)
        assert f.read(cnt) == zeropad_bytes(cnt, b'b')
        f.seek(hugefilesize)
        assert f.read() == b'c'

        def _truncate_and_ensure_start_sane(cnt):
            f.truncate(cnt)
            assert f.inode.size == cnt
            assert f.inode.stored_size == f.inode.size
            f.seek(0)
            s = f.read(const.BLOCK_SIZE_LIMIT)
            exp_len = int(min(const.BLOCK_SIZE_LIMIT, cnt))
            assert s == zeropad_bytes(exp_len, b'a')
            if cnt > middlish:
                f.seek(middlish)
                s = f.read(const.BLOCK_SIZE_LIMIT)
                exp_len = int(min(const.BLOCK_SIZE_LIMIT, cnt - middlish))
                assert s == zeropad_bytes(exp_len, b'b')

        _truncate_and_ensure_start_sane(middlish + 5)
        _truncate_and_ensure_start_sane(const.BLOCK_SIZE_LIMIT - 3)
        _truncate_and_ensure_start_sane(
            const.INTERNED_BLOCK_DATA_SIZE_LIMIT - 7)


def argument_parser():
    p = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('--cache-size', type=int, default=1024**3,
                   help='Maximum cache size used by storage')
    p.add_argument('--compress', '-c', action='store_true',
                   help='Compress the content opportunistically')
    p.add_argument('--debug', '-d', action='store_true',
                   help='Enable debugging')
    p.add_argument(
        '--filename', '-f',
        help='Filename to store the data in')
    p.add_argument('--interval', '-i',
                   type=int, default=10, help='flush interval')
    p.add_argument('--mountpoint', '-m',
                   default='/tmp/x',
                   help='Where the file should be mounted')
    p.add_argument('--salt', help='Salt to use')
    p.add_argument(
        '--password', '-p',
        help='Program to get the password from for encryption')
    p.add_argument('--workers', '-w',
                   default=1, type=int, help='number of threads to use')
    return p

if __name__ == '__main__':
    import subprocess
    # TBD - argument parsing?
    p = argument_parser()
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
        backend = st.SQLiteStorageBackend(codec=codec, filename=args.filename)
        storage = st.DelayedStorage(backend=backend)
        storage.maximum_cache_size = args.cache_size

    else:
        storage = st.DictStorage()

    forest = forest.Forest(storage)
    ops = ops.Operations(forest)
    fuse_options = set(llfuse.default_options)
    fuse_options.remove('nonempty')  # TBD..
    fuse_options.add('fsname=test_fs')
    # fuse_options.add('large_read') # n/a on OS X?
    # fuse_options.add('blksize=%d' % const.BLOCK_SIZE_LIMIT) # n/a on OS X?
    fuse_options.add('max_read=%d' % (const.BLOCK_SIZE_LIMIT * 10))
    fuse_options.add('max_write=%d' % (const.BLOCK_SIZE_LIMIT * 10))
    fuse_options.add('slow_statfs')
    # fuse_options.add('novncache') # this works but what does it do?
    # fuse_options.add('noattrcache')  # this works but what does it do?
    fuse_options.add('allow_other')
    if args.debug:
        fuse_options.add('debug')
    llfuse.init(ops, args.mountpoint, fuse_options)
    tl = [None, True]
    import threading

    is_closed = False

    def _run_flush_timer():
        if not tl[1]:
            return
        with llfuse.lock:
            ops.forest.flush()
        tl[0] = threading.Timer(args.interval, _run_flush_timer)
        tl[0].start()
    _run_flush_timer()
    try:
        sig = llfuse.main(workers=args.workers)
        tl[1] = False  # Even if the timer fires now, it should be nop
        if sig is None:
            llfuse.close()
            is_closed = True
    finally:
        if not is_closed:
            _debug('closing llfuse')
            llfuse.close(unmount=False)
            _debug('umount mountpoint (just in case)')
            subprocess.call(['umount', args.mountpoint])
        _debug('cancel flush timer')
        tl[0].cancel()
