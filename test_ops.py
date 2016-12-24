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
# Last modified: Sat Dec 24 07:17:24 2016 mstenber
# Edit time:     206 min
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
import stat

import pytest

import const
import forest
import llfuse
import ops
from storage import DictStorage

_debug = logging.getLogger(__name__).debug


class FakeStruct:

    def __init__(self, **kw):
        for k, v in kw.items():
            assert hasattr(self, k)
            setattr(self, k, v)


class RequestContextIsh(FakeStruct):
    uid = 0
    pid = 0
    gid = 0
    umask = 0o077  # self-only by default


class SetattrFieldsIsh(FakeStruct):
    update_uid = False
    update_gid = False
    update_mtime = False
    update_mode = False
    update_size = False


class OpsContext:

    def __init__(self):
        self.inodes = {b'.': llfuse.ROOT_INODE}
        storage = DictStorage()
        self.forest = forest.Forest(storage)
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
        assert stat.S_ISREG(attr.st_mode)
        self.ops.release(fd)

    def ensure_storage_matches_forest(self):
        f2 = forest.Forest(self.forest.storage)
        ops2 = ops.Operations(f2).init()
        todo = [(b'.',
                 self.ops.lookup(llfuse.ROOT_INODE, b'.', self.rctx_root),
                 ops2.lookup(llfuse.ROOT_INODE, b'.', self.rctx_root))]
        while todo:
            p, a1, a2 = todo.pop()
            _debug('considering %s', p)
            ad1 = attr_to_dict(a1)
            ad2 = attr_to_dict(a2)
            ad1.pop('st_ino')
            ad2.pop('st_ino')
            _debug(' ad1: %s', ad1)
            _debug(' ad2: %s', ad2)
            inode1 = self.forest.inodes.get_by_value(a1.st_ino)
            inode2 = f2.inodes.get_by_value(a2.st_ino)
            # TBD: What needs to be popped?
            assert ad1 == ad2
            if stat.S_ISDIR(a1.st_mode):
                # Moar TODO to be had! Yay
                for (n1, na1, o1), (n2, na2, o2) in zip(self.ops.readdir(a1.st_ino, 0),
                                                        ops2.readdir(a2.st_ino, 0)):
                    assert n1 == n2
                    na1 = self.ops.lookup(a1.st_ino, n1, self.rctx_root)
                    na2 = ops2.lookup(a2.st_ino, n2, self.rctx_root)
                    todo.append((b'%s/%s' % (p, n1), na1, na2))
            elif stat.S_ISREG(a1.st_mode):
                # Ensure that what we know underneath matches direntry..
                s = inode1.size
                assert s == inode2.size
                assert s == inode1.stored_size
                assert s == inode2.stored_size
                if s <= 10 * const.BLOCK_SIZE_LIMIT:
                    assert inode1.read(0, s) == inode2.read(0, s)
                else:
                    assert inode1.read(0, 1) == inode2.read(0, 1)
                    assert inode1.read(s // 2, 1) == inode2.read(s // 2, 1)
                    assert inode1.read(s - 1, 1) == inode2.read(s - 1, 1)
            self.ops.forget1(a1.st_ino)
            ops2.forget1(a2.st_ino)

    def inodes_get_counts(self):
        d = {}
        for n, inode in self.forest.inodes.value2object.items():
            d[n] = inode.refcnt
        return d

    def mkdir(self, parent_inode, name, ctx, *, mode=0o700):
        attr = self.ops.mkdir(parent_inode, name, mode, ctx)
        assert isinstance(attr, llfuse.EntryAttributes)
        self.inodes[name] = attr.st_ino
        assert stat.S_ISDIR(attr.st_mode)
        return attr


@pytest.fixture
def oc():
    r = OpsContext()
    pre_counts = r.inodes_get_counts()
    yield r
    r.forest.flush()
    assert not r.forest.fds.value2object
    post_counts = r.inodes_get_counts()
    assert pre_counts == post_counts
    r.ensure_storage_matches_forest()
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


def test_statvfs(oc):
    r = oc.ops.statfs(oc.rctx_user)
    assert r.f_blocks
    assert r.f_bavail


def test_symlink(oc):
    target = b'/user_file'
    a = oc.ops.symlink(llfuse.ROOT_INODE, b'x', target, oc.rctx_user)
    assert oc.ops.readlink(a.st_ino, oc.rctx_user) == target
    oc.ops.forget1(a.st_ino)


def test_symlink_over_nonowned_err(oc):
    target = b'/user_file'
    with pytest.raises(llfuse.FUSEError) as e:
        oc.ops.symlink(llfuse.ROOT_INODE, b'root_file', target, oc.rctx_user)
        assert False
    assert e.value.errno == errno.EPERM


def test_rename(oc):
    oc.ops.rename(llfuse.ROOT_INODE, b'user_dir',
                  llfuse.ROOT_INODE, b'x', oc.rctx_root)
    a = oc.ops.lookup(llfuse.ROOT_INODE, b'x', oc.rctx_root)
    oc.ops.forget1(a.st_ino)
    with pytest.raises(llfuse.FUSEError):
        oc.ops.lookup(llfuse.ROOT_INODE, b'user_dir', oc.rctx_root)


def test_rename_overwrite(oc):
    oc.ops.rename(llfuse.ROOT_INODE, b'user_dir',
                  llfuse.ROOT_INODE, b'user_file', oc.rctx_root)
    a = oc.ops.lookup(llfuse.ROOT_INODE, b'user_file', oc.rctx_root)
    assert stat.S_ISDIR(a.st_mode)
    oc.ops.forget1(a.st_ino)
    with pytest.raises(llfuse.FUSEError):
        oc.ops.lookup(llfuse.ROOT_INODE, b'user_dir', oc.rctx_root)


def test_lookup(oc):
    a = oc.ops.lookup(llfuse.ROOT_INODE, b'.', oc.rctx_root)
    assert stat.S_ISDIR(a.st_mode)
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


def testcreate_fail(oc):
    with pytest.raises(llfuse.FUSEError) as e:
        oc.create(llfuse.ROOT_INODE, b'root_file', oc.rctx_user)
    assert e.value.errno == errno.EPERM


def testopen_fail(oc):
    with pytest.raises(llfuse.FUSEError) as e:
        oc.ops.open(oc.inodes[b'root_file'], os.O_RDONLY, oc.rctx_user)
    assert e.value.errno == errno.EPERM


def testmkdir_fail_perm(oc):
    with pytest.raises(llfuse.FUSEError) as e:
        oc.ops.mkdir(llfuse.ROOT_INODE, b'root_dir', 0, oc.rctx_user)
    assert e.value.errno == errno.EEXIST


def testmkdir_ok(oc):
    a = oc.mkdir(llfuse.ROOT_INODE, b'x', oc.rctx_user)
    oc.ops.forget1(a.st_ino)


def testrmdir_fail_perm(oc):
    with pytest.raises(llfuse.FUSEError) as e:
        oc.ops.rmdir(llfuse.ROOT_INODE, b'root_dir', oc.rctx_user)
    assert e.value.errno == errno.EPERM


def testrmdir_fail_notempty(oc):
    with pytest.raises(llfuse.FUSEError) as e:
        oc.ops.rmdir(llfuse.ROOT_INODE, b'root_dir', oc.rctx_root)
    assert e.value.errno == errno.ENOTEMPTY


def testrmdir_ok(oc):
    a = oc.mkdir(llfuse.ROOT_INODE, b'x', oc.rctx_user)
    oc.ops.rmdir(llfuse.ROOT_INODE, b'x', oc.rctx_user)
    oc.ops.forget1(a.st_ino)

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
    with pytest.raises(llfuse.FUSEError) as e:
        oc.ops.getxattr(llfuse.ROOT_INODE, b'foo', oc.rctx_root)
    assert e.value.errno == errno.ENOATTR
    assert list(oc.ops.listxattr(llfuse.ROOT_INODE, oc.rctx_root)) == []
    oc.ops.setxattr(llfuse.ROOT_INODE, b'foo', b'bar', oc.rctx_root)
    assert list(oc.ops.listxattr(llfuse.ROOT_INODE, oc.rctx_root)) == [b'foo']
    oc.ops.getxattr(llfuse.ROOT_INODE, b'foo', oc.rctx_root) == b'bar'
    oc.ops.setxattr(llfuse.ROOT_INODE, b'baz', b'x', oc.rctx_root)
    oc.ops.removexattr(llfuse.ROOT_INODE, b'foo', oc.rctx_root)
    assert list(oc.ops.listxattr(llfuse.ROOT_INODE, oc.rctx_root)) == [b'baz']


def test_mknod_c(oc):
    a = oc.ops.mknod(llfuse.ROOT_INODE, b'cdev',
                     stat.S_IFCHR, 42, oc.rctx_user)
    assert a.st_rdev == 42
    oc.ops.forget1(a.st_ino)
    a = oc.ops.mknod(llfuse.ROOT_INODE, b'bdev',
                     stat.S_IFBLK, 43, oc.rctx_user)
    assert a.st_rdev == 43
    oc.ops.forget1(a.st_ino)
    a = oc.ops.mknod(llfuse.ROOT_INODE, b'regfile',
                     stat.S_IFREG, 44, oc.rctx_user)
    assert not a.st_rdev
    oc.ops.forget1(a.st_ino)


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


@pytest.mark.parametrize('filename,is_root,fkb,v', [
    # uid / gid change as root is ok
    (b'user_file', True, 'uid', 0),
    (b'user_file', True, 'gid', 0),
    pytest.mark.xfail((b'user_file', False, 'uid', 0)),
    pytest.mark.xfail((b'user_file', False, 'gid', 0)),
    (b'user_file', False, 'mtime', 42),
    pytest.mark.xfail((b'root_file', False, 'mtime', 42)),
    (b'user_file', False, 'mode', 0),
    (b'user_file', False, 'size', 1),
])
def test_setattr(oc, filename, is_root, fkb, v):
    ctx = is_root and oc.rctx_root or oc.rctx_user
    fk = 'update_%s' % fkb
    k = {'uid': 'st_uid', 'gid': 'st_gid', 'mtime': 'st_mtime_ns',
         'mode': 'st_mode', 'size': 'st_size'}[fkb]
    f = SetattrFieldsIsh()
    setattr(f, fk, True)
    a = llfuse.EntryAttributes()
    setattr(a, k, v)
    a = oc.ops.setattr(oc.inodes[filename], a, f, 0, ctx)
    assert getattr(a, k) == v
