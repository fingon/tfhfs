#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_ops.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Wed Aug 17 10:39:05 2016 mstenber
# Last modified: Thu Dec 15 20:13:26 2016 mstenber
# Edit time:     129 min
#
"""

This module implements tests of the ops module (which is
llfuse.Operations subclass). In theory, this stuff could be usable by
other llfuse using projects as well.

TBD: Think if refactoring this to some sort of 'llfusetester' module
would make sense.

"""

import errno
import logging
import os

import pytest

import const
import forest
import llfuse
import ops
from storage import NopBlockCodec, SQLiteStorage, TypedBlockCodec

_debug = logging.getLogger(__name__).debug


class RequestContextIsh:
    uid = 0
    pid = 0
    gid = 0
    umask = 0o777

    def __init__(self, **kw):
        for k, v in kw.items():
            assert hasattr(self, k)
            setattr(self, k, v)


class OpsContext:

    def __init__(self):
        self.inodes = {b'.': llfuse.ROOT_INODE}
        storage = SQLiteStorage(codec=TypedBlockCodec(NopBlockCodec()))
        self.forest = forest.Forest(storage, llfuse.ROOT_INODE)
        self.ops = ops.Operations(self.forest)
        self.rctx_root = RequestContextIsh()
        self.rctx_user = RequestContextIsh(uid=42, gid=7, pid=123)
        self.ops.init()

        # Create root-owned directory + file + file in directory
        self.mkdir(llfuse.ROOT_INODE, b'root_dir', self.rctx_root)
        self.create(llfuse.ROOT_INODE, b'root_file', self.rctx_root,
                    data=b'root')
        self.create(self.inodes[b'root_dir'], b'root_file_in', self.rctx_root,
                    data=b'root2')

        # Create user-owned directory + file
        self.mkdir(llfuse.ROOT_INODE, b'user_dir', self.rctx_user)
        self.create(llfuse.ROOT_INODE, b'user_file', self.rctx_user,
                    data=b'user')
        self.create(self.inodes[b'user_dir'], b'user_file_in', self.rctx_user,
                    data=b'user2')

        # Ensure that stuff with 0 refcnt is gone?
        self.forest.flush()

    def create(self, parent_inode, name, ctx, *,
               mode=0o600, flags=os.O_WRONLY, data=b''):
        r = self.ops.create(parent_inode, name, mode, flags, ctx)
        (fd, attr) = r
        assert isinstance(attr, llfuse.EntryAttributes)
        self.inodes[name] = attr.st_ino
        if data:
            r = self.ops.write(fd, 0, data)
            assert r == len(data)
        self.ops.release(fd)

    def get_inode_counts(self):
        d = {}
        for n, inode in self.forest._value2inode.items():
            d[n] = inode.refcnt
        return d

    def mkdir(self, parent_inode, name, ctx, *, mode=0o700):
        attr = self.ops.mkdir(parent_inode, name, mode, ctx)
        assert isinstance(attr, llfuse.EntryAttributes)
        self.inodes[name] = attr.st_ino


@pytest.fixture
def oc():
    r = OpsContext()
    pre_counts = r.get_inode_counts()
    yield r
    assert not r.forest.fd2o
    post_counts = r.get_inode_counts()
    assert pre_counts == post_counts
    r.ops.destroy()


def attr_to_dict(a):
    assert isinstance(a, llfuse.EntryAttributes)
    return {k: getattr(a, k) for k in const.ATTR_KEYS}


def attr_equal(a1, a2):
    a1 = attr_to_dict(a1)
    a2 = attr_to_dict(a2)
    return a1 == a2


def test_dir(oc):
    fd = oc.ops.opendir(oc.inodes[b'root_dir'], oc.rctx_root)
    l = list(x[0] for x in oc.ops.readdir(fd, 0))
    assert l == [b'root_file_in']
    oc.ops.releasedir(fd)


def test_rename(oc):
    oc.ops.rename(llfuse.ROOT_INODE, b'user_dir',
                  llfuse.ROOT_INODE, b'x', oc.rctx_root)
    a = oc.ops.lookup(llfuse.ROOT_INODE, b'x', oc.rctx_root)
    oc.ops.forget1(a.st_ino)
    try:
        oc.ops.lookup(llfuse.ROOT_INODE, b'user_dir', oc.rctx_root)
        assert False
    except llfuse.FUSEError:
        pass


def test_lookup(oc):
    a = oc.ops.lookup(llfuse.ROOT_INODE, b'.', oc.rctx_root)
    a2 = oc.ops.lookup(llfuse.ROOT_INODE, b'..', oc.rctx_root)
    assert attr_equal(a, a2)
    a3 = oc.ops.lookup(oc.inodes[b'user_dir'], b'..', oc.rctx_root)
    assert attr_equal(a, a3)
    a4 = oc.ops.lookup(oc.inodes[b'user_dir'], b'.', oc.rctx_root)
    assert not attr_equal(a, a4)
    oc.ops.forget([(a4.st_ino, 1), (a.st_ino, 3)])


@pytest.mark.parametrize('filename,is_root,expect_success', [
    (b'root_dir', True, True),
    (b'root_dir', False, False),
    (b'user_dir', False, True),
    (b'user_dir', True, True),
    (b'root_file', True, True),
    (b'root_file', False, False),
    (b'user_file', False, True),
    (b'user_file', True, True),
    (b'.', True, True),
    (b'.', False, True),
])
def test_access(oc, filename, is_root, expect_success):
    ctx = is_root and oc.rctx_root or oc.rctx_user
    _debug('is_root:%s uid:%d', is_root, ctx.uid)
    r = oc.ops.access(oc.inodes[filename], os.R_OK, ctx)
    assert r == expect_success

# successful create implicitly tested in create of setUp


@pytest.mark.xfail(raises=llfuse.FUSEError)
def testcreate_fail(oc):
    oc.create(llfuse.ROOT_INODE, b'root_file', oc.rctx_user)


@pytest.mark.xfail(raises=llfuse.FUSEError)
def testopen_fail(oc):
    oc.ops.open(oc.inodes[b'root_file'], os.O_RDONLY, oc.rctx_user)


@pytest.mark.xfail(raises=llfuse.FUSEError)
def testmkdir_fail(oc):
    oc.ops.mkdir(llfuse.ROOT_INODE, b'root_dir', 0, oc.rctx_user)


@pytest.mark.xfail(raises=llfuse.FUSEError)
def testrmdir_fail(oc):
    oc.ops.rmdir(llfuse.ROOT_INODE, b'root_dir', oc.rctx_user)

# destroy implicitly tested in tearDown


def test_flush(oc):
    # TBD - it should not have any real semantics?
    pass

# mkdir implicitly tested in mkdir


def test_basic_file_io(oc):
    r = oc.ops.create(llfuse.ROOT_INODE, b'x', 0, os.O_WRONLY, oc.rctx_root)
    (fh, attr) = r
    assert isinstance(attr, llfuse.EntryAttributes)
    oc.ops.release(fh)

    fd = oc.ops.open(attr.st_ino, os.O_RDONLY, oc.rctx_root)
    assert isinstance(fd, int)

    oc.ops.fsyncdir(llfuse.ROOT_INODE, False)
    oc.ops.fsyncdir(llfuse.ROOT_INODE, True)

    oc.ops.fsync(fd, False)
    oc.ops.fsync(fd, True)

    oc.ops.flush(fd)  # should be nop?

    r = oc.ops.read(fd, 0, 123)
    assert not r

    r = oc.ops.write(fd, 0, b'foo')
    assert r == 3

    r = oc.ops.read(fd, 0, 123)
    assert r == b'foo'

    oc.ops.fsync(fd, False)
    oc.ops.fsync(fd, True)

    oc.ops.release(fd)

    oc.ops.forget1(attr.st_ino)  # from initial create


@pytest.mark.parametrize('inode,user_ctx', [
    (llfuse.ROOT_INODE, True),
    (llfuse.ROOT_INODE, False),
])
def test_getattr(oc, inode, user_ctx):
    rctx = user_ctx and oc.rctx_user or oc.rctx_root
    r = oc.ops.getattr(inode, rctx)
    assert isinstance(r, llfuse.EntryAttributes)
    assert r.st_ino == inode

    # TBD: Test more?


def test_basic_xattr(oc):
    try:
        oc.ops.getxattr(llfuse.ROOT_INODE, b'foo', oc.rctx_root)
        assert False
    except llfuse.FUSEError as e:
        assert e.errno == errno.ENOATTR
    assert list(oc.ops.listxattr(llfuse.ROOT_INODE, oc.rctx_root)) == []
    oc.ops.setxattr(llfuse.ROOT_INODE, b'foo', b'bar', oc.rctx_root)
    assert list(oc.ops.listxattr(llfuse.ROOT_INODE, oc.rctx_root)) == [b'foo']
    oc.ops.getxattr(llfuse.ROOT_INODE, b'foo', oc.rctx_root) == b'bar'
    oc.ops.setxattr(llfuse.ROOT_INODE, b'baz', b'x', oc.rctx_root)
    oc.ops.removexattr(llfuse.ROOT_INODE, b'foo', oc.rctx_root)
    assert list(oc.ops.listxattr(llfuse.ROOT_INODE, oc.rctx_root)) == [b'baz']


def test_ensure_full_implementation():
    """ Make sure we implement all methids defined in llfuse.Operations in
    ops.Opeartions. """
    ignored_always = {'__dict__', '__weakref__'}
    ignored_base = {'stacktrace',  # utility
                    } | ignored_always
    ignored_ours = {'x'} | ignored_always
    vars1 = set(vars(llfuse.Operations).keys()).difference(ignored_base)
    vars2 = set(vars(ops.Operations).keys()).difference(ignored_ours)
    print(vars1, vars2)
    assert vars1.difference(vars2) == set()
    # assert vars2.difference(vars1) == set() # we don't care about extra
