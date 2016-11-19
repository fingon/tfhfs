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
# Last modified: Sat Nov 19 10:42:04 2016 mstenber
# Edit time:     41 min
#
"""

This module implements tests of the ops module (which is
llfuse.Operations subclass). In theory, this stuff could be usable by
other llfuse using projects as well.

TBD: Think if refactoring this to some sort of 'llfusetester' module
would make sense.

"""

import os
import unittest

import ddt
import pytest

import llfuse
import ops


@ddt.ddt
class OpsTester(unittest.TestCase):

    def _create(self, parent_inode, name, ctx, *, mode=0, flags=0, data=b''):
        r = self.ops.create(parent_inode, name, mode, flags, ctx)
        (fd, attr) = r
        assert isinstance(attr, llfuse.EntryAttributes)
        self.inode[name] = attr.st_ino

        if data:
            r = self.ops.write(fd, 0, data)
            assert r == len(data)

        self.ops.release(fd)

    def _mkdir(self, parent_inode, name, ctx, *, mode=0):
        attr = self.ops.mkdir(parent_inode, name, mode, ctx)
        assert isinstance(attr, llfuse.EntryAttributes)
        self.inode[name] = attr.st_ino

    def setUp(self):
        self.inodes = {b'.': llfuse.ROOT_INODE}

        self.ops = ops.Operations()
        self.rctx_root = llfuse.RequestContext()
        self.rctx_user = llfuse.RequestContext(uid=42, gid=7, pid=123)
        self.ops.init()

        # Create user-owned directory + file
        self._mkdir(llfuse.ROOT_INODE, b'root_dir', self.rctx_root)
        self._create(llfuse.ROOT_INODE, b'root_file', self.rctx_root,
                     data=b'root')

        # Create root-owned directory + file
        self._mkdir(llfuse.ROOT_INODE, b'user_dir', self.rctx_user)
        self._create(llfuse.ROOT_INODE, b'user_file', self.rctx_user,
                     data=b'user')

    def tearDown(self):
        self.ops.destroy()
        # TBD: Ensure no inodes around

    @pytest.mark.xfail(raises=llfuse.FUSEError)
    @ddt.data(
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
    )
    @ddt.unpack
    def test_access(self, filename, is_root, expect_success):
        ctx = is_root and self.rctx_root or self.rctx_user
        r = self.ops.access(self.inodes[filename], os.R_OK, ctx)
        assert r == expect_success

    # successful create implicitly tested in _create of setUp
    @pytest.mark.xfail(raises=llfuse.FUSEError)
    def test_create_fail(self):
        self._create(llfuse.ROOT_INODE, b'root_file', self.rctx_user)

    # destroy implicitly tested in tearDown

    @pytest.mark.xfail(raises=llfuse.FUSEError)
    def test_flush(self):
        # TBD - it should not have any real semantics?
        pass

    # mkdir implicitly tested in _mkdir
    @pytest.mark.xfail(raises=llfuse.FUSEError)
    def test_basic_file_io(self):
        r = self.ops.create(llfuse.ROOT_INODE, 'x', 0, 0, self.rctx_root)
        (fh, attr) = r
        assert isinstance(attr, llfuse.EntryAttributes)

        fd = self.ops.open(attr.st_ino, 0, self.rctx_root)
        assert isinstance(fd, int)

        self.ops.fsync(fh, False)
        self.ops.fsync(fh, True)

        self.ops.flush(fd)  # should be nop?

        r = self.ops.read(fd, 0, 123)
        assert not r

        r = self.ops.write(fd, 0, b'foo')
        assert r == 3

        r = self.ops.read(fd, 0, 123)
        assert r == b'foo'

        self.ops.fsync(fh, False)
        self.ops.fsync(fh, True)

        self.ops.release(fd)

        self.ops.forget((attr.st_ino, 1))

    @pytest.mark.xfail(raises=llfuse.FUSEError)
    def test_getattr(self, inode, ctx):
        r = self.ops.getattr(llfuse.ROOT_INODE, self.rctx_root)
        assert isinstance(r, llfuse.EntryAttributes)
        assert r.st_ino == llfuse.ROOT_INODE

        # TBD: Test more?

    @pytest.mark.xfail(raises=llfuse.FUSEError)
    def test_basic_xattr(self):
        try:
            self.ops.getxattr(llfuse.ROOT_INODE, b'foo', self.rctx_root)
            raise
        except llfuse.FUSEError as e:
            assert e.errno_ == errno.ENOATTR
