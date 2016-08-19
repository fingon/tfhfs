#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: ops.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Tue Aug 16 12:56:24 2016 mstenber
# Last modified: Fri Aug 19 07:08:34 2016 mstenber
# Edit time:     13 min
#
"""

This is llfuse Operations subclass that implements tfhfs core
filesystem using the forest module and appropriate storage backend.

"""

from errno import ENOSYS

import llfuse


class Operations(llfuse.Operations):
    _initialized = False

    # Lifecycle functions

    def init(self):
        assert not self._initialized
        self._initialized = True

    def destroy(self):
        assert self._initialized
        del self._initialized

    # Normal client API

    def access(self, inode, mode, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def create(self, parent_inode, name, mode, flags, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def flush(self, fh):
        assert self._initialized
        assert isinstance(fh, int)
        raise llfuse.FUSEError(ENOSYS)

    def forget(self, inode_list):
        assert self._initialized
        for inode, nlookup in inode_list:
            # reduce reference count of 'inode' by 'nlookup'
            pass
        raise llfuse.FUSEError(ENOSYS)

    def fsync(self, fh, datasync):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def getattr(self, inode, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def getxattr(self, inode, name, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def link(self, inode, new_parent_inode, new_name, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def listxattr(self, inode, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def lookup(self, parent_inode, name, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def mkdir(self, parent_inode, name, mode, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def mknod(self, parent_inode, name, mode, rdev, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def open(self, inode, flags, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def opendir(self, inode, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def read(self, fh, off, size):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def readdir(self, fh, off):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def readlink(self, inode, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def release(self, fh):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def releasedir(self, fh):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def removexattr(self, inode, name, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def rename(self, parent_inode_old, name_old, parent_inode_new,
               name_new, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def rmdir(self, parent_inode, name, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def setattr(self, inode, attr, fields, fh, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def setxattr(self, inode, attr, fields, fh, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def statfs(self, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def symlink(self, parent_inode, name, target, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def unlink(self, parent_inode, name, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def write(self, fh, off, buf):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)
