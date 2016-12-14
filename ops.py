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
# Last modified: Thu Dec 15 07:41:22 2016 mstenber
# Edit time:     144 min
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

import logging
import os
from errno import EEXIST, ENOATTR, ENOENT, ENOSYS, ENOTEMPTY

import forest_nodes
import llfuse

_debug = logging.getLogger(__name__).debug


def assert_or_errno(stmt, err):
    if not stmt:
        raise llfuse.FUSEError(err)


class Operations(llfuse.Operations):

    _initialized = False

    def __init__(self, forest):
        self.forest = forest
        llfuse.Operations.__init__(self)

    def _leaf_attributes(self, leaf_node):
        entry = llfuse.EntryAttributes()
        for attr in ('st_mode', 'st_nlink', 'st_uid', 'st_gid',
                     'st_rdev', 'st_size', 'st_atime_ns', 'st_mtime_ns',
                     'st_ctime_ns'):
            v = leaf_node.data.get(attr)
            if v is not None:
                setattr(entry, attr, v)
        return entry

    def _inode_attributes(self, inode):
        de = inode.direntry
        assert isinstance(de, forest_nodes.DirectoryEntry)
        entry = self._leaf_attributes(de)
        entry.st_ino = inode.value
        return entry

    # Lifecycle functions

    def init(self):
        assert not self._initialized
        self._initialized = True

    def destroy(self):
        assert self._initialized

        # Store current state to disk, and then restart from scratch.
        # Running forget-equivalents does not seem appealing.
        self.forest.flush()
        self.forest.init()

        del self._initialized

    # Normal client API

    def access(self, inode, mode, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def create(self, parent_inode, name, mode, flags, ctx):
        assert self._initialized
        _debug('create @i:%s %s m%o f:0x%x %s',
               parent_inode, name, mode, flags, ctx)
        n = self.forest.getdefault_inode_by_value(parent_inode)
        assert_or_errno(n, ENOENT)
        file_inode = self.forest.lookup(n, name)
        if file_inode:
            assert_or_errno(not (flags & os.O_EXCL), EEXIST)
        else:
            file_inode = self.forest.create_file(n, name)
        fd = file_inode.open(flags)
        return fd, self._inode_attributes(file_inode)
        # return fd, self._leaf_attributes(file_inode.leaf_node)

    def flush(self, fh):
        assert self._initialized
        assert isinstance(fh, int)
        pass  # we write always immediately, just fsync is slow

    def forget(self, inode_list):
        assert self._initialized
        for inode, nlookup in inode_list:
            # reduce reference count of 'inode' by 'nlookup'
            self.forget1(inode, nlookup)

    def forget1(self, inode, count=1):
        self.forest.get_inode_by_value(inode).deref(count)

    def fsync(self, fh, datasync):
        assert self._initialized
        self.forest.lookup_fd(fh).flush()

    def fsyncdir(self, fh, datasync):
        assert self._initialized
        inode = self.forest.getdefault_inode_by_value(fh)
        assert inode
        inode.flush()

    def getattr(self, inode, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def getxattr(self, inode, name, ctx):
        assert self._initialized
        inode = self.forest.getdefault_inode_by_value(inode)
        xa = inode.direntry.data.get('xattr')
        assert_or_errno(xa, ENOATTR)
        v = xa.get(name)
        assert_or_errno(v, ENOATTR)
        return v

    def link(self, inode, new_parent_inode, new_name, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def listxattr(self, inode, ctx):
        assert self._initialized
        inode = self.forest.getdefault_inode_by_value(inode)
        return inode.direntry.data.get('xattr', {}).keys()

    def lookup(self, parent_inode, name, ctx):
        assert self._initialized
        n = self.forest.getdefault_inode_by_value(parent_inode)
        assert_or_errno(n, ENOENT)
        if name == b'.':
            pass
        elif name == b'..':
            cn = n.leaf_node
            if cn:
                gp_inode = self.forest.getdefault_inode_by_parent(cn.root)
                if gp_inode:
                    return self.lookup(gp_inode.value, b'.', ctx)
        else:
            n = self.forest.lookup(n, name)
            assert_or_errno(n, ENOENT)
        assert n
        return self._inode_attributes(n)

    def mkdir(self, parent_inode, name, mode, ctx):
        assert self._initialized
        inode = self.forest.getdefault_inode_by_value(parent_inode)
        assert_or_errno(inode, ENOENT)
        cn = inode.node.search_name(name)
        assert_or_errno(not cn, EEXIST)
        # TBD: access
        self.forest.create_dir(inode, name=name)
        return self.lookup(parent_inode, name, ctx)

    def mknod(self, parent_inode, name, mode, rdev, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def open(self, inode, flags, ctx):
        assert self._initialized
        _debug('open i:%d f:0x%x %s', inode, flags, ctx)
        inode = self.forest.getdefault_inode_by_value(inode)
        assert_or_errno(inode, ENOENT)
        return inode.open(flags)

    def opendir(self, inode, ctx):
        assert self._initialized
        inode = self.forest.getdefault_inode_by_value(inode)
        assert_or_errno(inode, ENOENT)
        inode.ref()
        return inode.value

    def read(self, fh, off, size):
        assert self._initialized
        return self.forest.lookup_fd(fh).read(off, size)

    def readdir(self, fh, off):
        assert self._initialized
        inode = self.forest.getdefault_inode_by_value(fh)
        assert inode
        pln = None
        for i, ln in enumerate(inode.node.get_leaves()):
            # Additions may screw up the tree bit
            if (pln is not None and pln.key > ln.key) or not ln.name:
                pass
            else:
                pln = ln
                if i >= off:
                    t = (ln.name, self._leaf_attributes(ln), i)
                    yield t

    def readlink(self, inode, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def release(self, fh):
        assert self._initialized
        self.forest.lookup_fd(fh).close()

    def releasedir(self, fh):
        assert self._initialized
        inode = self.forest.getdefault_inode_by_value(fh)
        assert inode
        inode.deref()

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
        n = self.forest.getdefault_inode_by_value(parent_inode)
        assert_or_errno(n, ENOENT)
        n = self.forest.lookup(n, name)
        assert_or_errno(n and n.leaf_node.is_dir and n.node, ENOENT)
        assert_or_errno(not n.children, ENOTEMPTY)
        n.leaf_node.root.remove(n.leaf_node)

    def setattr(self, inode, attr, fields, fh, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def setxattr(self, inode, name, value, ctx):
        assert self._initialized
        inode = self.forest.getdefault_inode_by_value(inode)
        xa = inode.direntry.data.get('xattr', {})
        xa[name] = value
        inode.direntry.set_data('xattr', xa)

    def statfs(self, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def symlink(self, parent_inode, name, target, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def unlink(self, parent_inode, name, ctx):
        assert self._initialized
        n = self.forest.getdefault_inode_by_value(parent_inode)
        assert_or_errno(n, ENOENT)
        cn = n.node.search_name(name)
        assert_or_errno(cn and cn.is_file, ENOENT)
        n.node.remove(cn)

    def write(self, fh, off, buf):
        assert self._initialized
        return self.forest.lookup_fd(fh).write(off, buf)
