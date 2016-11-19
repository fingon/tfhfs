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
# Last modified: Sat Nov 19 12:36:58 2016 mstenber
# Edit time:     35 min
#
"""

This is llfuse Operations subclass that implements tfhfs core
filesystem using the forest module and appropriate storage backend.

This should implement methods in

http://pythonhosted.org/llfuse/operations.html

using this as bonus

http://pythonhosted.org/llfuse/fuse_api.html

 llfuse.invalidate_inode(inode, attr_only=False)
 - forget cached attributes + data (if attr_only not set) for inode

 llfuse.invalidate_entry(inode, name)

 - invalidate dentry within directory within inode inode



"""

from errno import EEXIST, ENOENT, ENOSYS, EPERM

import llfuse


def assert_or_errno(stmt, err):
    if not stmt:
        raise llfuse.FUSEError(err)


class Operations(llfuse.Operations):

    _initialized = False

    def __init__(self, forest):
        self.forest = forest
        llfuse.Operations.__init__(self)

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

    def fsyncdir(self, fh, datasync):
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
        n = self.forest.get_inode(parent_inode)
        assert_or_errno(n, ENOENT)
        # TBD: '.', '..'
        cn = n.search_name(name)
        assert_or_errno(cn, ENOENT)

        raise llfuse.FUSEError(ENOSYS)

    def mkdir(self, parent_inode, name, mode, ctx):
        assert self._initialized
        n = self.forest.get_inode(parent_inode)
        assert_or_errno(n, ENOENT)
        cn = n.search_name(name)
        assert_or_errno(not cn and name not in [b'.', b'..'], EEXIST)
        # TBD: access
        self.forest.create_dir(n, name=name)
        return self.lookup(parent_inode, name, ctx)

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
