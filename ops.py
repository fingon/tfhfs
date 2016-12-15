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
# Last modified: Thu Dec 15 22:22:03 2016 mstenber
# Edit time:     232 min
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
import stat
from errno import EEXIST, ENOATTR, ENOENT, ENOSYS, ENOTEMPTY, EPERM

import forest_nodes
import llfuse

_debug = logging.getLogger(__name__).debug

WX_OK = os.W_OK | os.X_OK
RX_OK = os.R_OK | os.X_OK


def assert_or_errno(stmt, err):
    if not stmt:
        raise llfuse.FUSEError(err)


def _flags_to_perm(flags):
    if flags & os.O_RDWR:
        p = os.R_OK | os.W_OK
    elif flags & os.O_WRONLY:
        p = os.W_OK
    elif not flags:
        p = os.R_OK
    else:
        p = os.R_OK | os.W_OK
    return p


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
                _debug('%s = %s', attr, v)
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

    def _de_access(self, de, mode, ctx):
        dedata = de.data
        target_mode = dedata.get('st_mode', 0)
        target_uid = dedata.get('st_uid', 0)
        target_gid = dedata.get('st_gid', 0)
        perms = target_mode & 0x7
        if ctx.gid == target_gid:
            perms |= (target_mode >> 3) & 0x7
        if ctx.uid == target_uid:
            perms |= (target_mode >> 6) & 0x7
        r = (perms & mode) == mode or not ctx.uid
        _debug('uid:%d perms:%d, target:%d => %s', ctx.uid, perms, mode, r)
        return r

    def access(self, inode, mode, ctx):
        assert self._initialized
        assert mode  # F_OK = 0, but should not show up(?)
        inode = self.forest.get_inode_by_value(inode)
        return self._de_access(inode.direntry, mode, ctx)

    def _set_de_perms_from_mode_ctx(self, de, mode, ctx):
        de.set_data('st_uid', ctx.uid)
        de.set_data('st_gid', ctx.gid)
        if mode >= 0:
            de.set_data('st_mode', mode & ctx.umask)

    def create(self, parent_inode, name, mode, flags, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, _flags_to_perm(flags), ctx),
                        EPERM)
        _debug('create @i:%s %s m%o f:0x%x %s',
               parent_inode, name, mode, flags, ctx)
        n = self.forest.get_inode_by_value(parent_inode)
        try:
            file_inode = self.forest.lookup(n, name)
            if file_inode:
                assert_or_errno(not (flags & os.O_EXCL), EEXIST)
                assert_or_errno(self.access(file_inode.value,
                                            _flags_to_perm(flags), ctx), EPERM)
            else:
                file_inode = self.forest.create_file(n, name)
                self._set_de_perms_from_mode_ctx(file_inode.direntry, mode,
                                                 ctx)
            fd = file_inode.open(flags)
        except:
            file_inode.deref()
            # Otherwise we can return it and increment the refcnt
            raise
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
        self.forest.get_inode_by_value(fh).flush()

    def getattr(self, inode, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, os.R_OK, ctx), EPERM)
        return self._inode_attributes(self.forest.get_inode_by_value(inode))

    def getxattr(self, inode, name, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, os.R_OK, ctx), EPERM)
        inode = self.forest.get_inode_by_value(inode)
        xa = inode.direntry.data.get('xattr')
        assert_or_errno(xa, ENOATTR)
        v = xa.get(name)
        assert_or_errno(v, ENOATTR)
        return v

    def link(self, inode, new_parent_inode, new_name, ctx):
        assert self._initialized
        assert_or_errno(self.access(new_parent_inode, WX_OK, ctx), EPERM)
        inode = self.forest.get_inode_by_value(inode)
        assert_or_errno(not inode.leaf_node, ENOSYS)
        parent_inode = self.forest.get_inode_by_value(new_parent_inode)
        n = parent_inode.node.search_name(new_name)
        if n:
            self.unlink(new_parent_inode, new_name, ctx)
        rn = parent_inode.node
        leaf = rn.leaf_class(self.forest, name=new_name)
        self.forest.get_inode_by_node(rn).set_node(rn.add(leaf))
        inode.set_leaf_node(leaf)

    def listxattr(self, inode, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, os.R_OK, ctx), EPERM)
        inode = self.forest.get_inode_by_value(inode)
        return inode.direntry.data.get('xattr', {}).keys()

    def lookup(self, parent_inode, name, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, os.X_OK, ctx), EPERM)
        n = self.forest.get_inode_by_value(parent_inode)
        if name == b'.':
            n.ref()
        elif name == b'..':
            cn = n.leaf_node
            if cn:
                n = self.forest.get_inode_by_node(cn.root).ref()
            else:
                n.ref()
        else:
            n = self.forest.lookup(n, name)
            assert_or_errno(n, ENOENT)
        assert n
        return self._inode_attributes(n)

    def mkdir(self, parent_inode, name, mode, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, WX_OK, ctx), EPERM)
        inode = self.forest.get_inode_by_value(parent_inode)
        cn = inode.node.search_name(name)
        assert_or_errno(not cn, EEXIST)
        dir_inode = self.forest.create_dir(inode, name=name,
                                           mode=(mode & ctx.umask))
        self._set_de_perms_from_mode_ctx(dir_inode.direntry, -1, ctx)
        return self._inode_attributes(dir_inode)

    def mknod(self, parent_inode, name, mode, rdev, ctx):
        assert self._initialized
        fd, a = self.create(parent_inode, name, mode,
                            os.O_TRUNC | os.O_CREAT, ctx)
        inode = self.forest.get_inode_by_value(a.st_ino)
        if stat.S_ISCHR(mode) or stat.S_ISBLK(mode):
            inode.direntry.set_data('st_rdev', rdev)
        self.release(fd)
        return self._inode_attributes(inode)

    def open(self, inode, flags, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, _flags_to_perm(flags), ctx), EPERM)
        _debug('open i:%d f:0x%x %s', inode, flags, ctx)
        return self.forest.get_inode_by_value(inode).open(flags)

    def opendir(self, inode, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, RX_OK, ctx), EPERM)
        inode = self.forest.get_inode_by_value(inode)
        inode.ref()
        return inode.value

    def read(self, fh, off, size):
        assert self._initialized
        return self.forest.lookup_fd(fh).read(off, size)

    def readdir(self, fh, off):
        assert self._initialized
        inode = self.forest.get_inode_by_value(fh)
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
        assert_or_errno(self.access(inode, os.R_OK, ctx), EPERM)
        raise llfuse.FUSEError(ENOSYS)  # TBD P2

    def release(self, fh):
        assert self._initialized
        self.forest.lookup_fd(fh).close()

    def releasedir(self, fh):
        assert self._initialized
        self.forest.get_inode_by_value(fh).deref()

    def removexattr(self, inode, name, ctx):
        assert_or_errno(self.access(inode, os.W_OK, ctx), EPERM)
        inode = self.forest.get_inode_by_value(inode)
        xa = inode.direntry.data.get('xattr', {})
        assert_or_errno(name in xa, ENOATTR)
        del xa[name]
        inode.direntry.set_data('xattr', xa)

    def rename(self, parent_inode_old, name_old, parent_inode_new,
               name_new, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode_old, WX_OK, ctx), EPERM)
        assert_or_errno(self.access(parent_inode_new, WX_OK, ctx), EPERM)
        parent_inode_old = self.forest.get_inode_by_value(parent_inode_old)
        parent_inode_new = self.forest.get_inode_by_value(parent_inode_new)
        n = self.forest.lookup(parent_inode_old, name_old)
        assert_or_errno(n, ENOENT)
        try:
            self.unlink(parent_inode_old.value, name_old, ctx, allow_any=True)
            self.link(n.value, parent_inode_new.value, name_new, ctx)
        finally:
            n.deref()

    def rmdir(self, parent_inode, name, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, WX_OK, ctx), EPERM)
        pn = self.forest.get_inode_by_value(parent_inode)
        n = self.forest.lookup(pn, name)
        assert_or_errno(n, ENOENT)
        try:
            assert_or_errno(n.leaf_node.is_dir, ENOENT)
            assert_or_errno(self._de_access(n.leaf_node, WX_OK, ctx), EPERM)
            assert_or_errno(not n.node.children, ENOTEMPTY)
            n.leaf_node.root.remove(n.leaf_node)
            # TBD: Also check the subdir permission?
        finally:
            n.deref()

    def setattr(self, inode, attr, fields, fh, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, os.W_OK, ctx), EPERM)
        raise llfuse.FUSEError(ENOSYS)

    def setxattr(self, inode, name, value, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, os.W_OK, ctx), EPERM)
        inode = self.forest.get_inode_by_value(inode)
        xa = inode.direntry.data.get('xattr', {})
        xa[name] = value
        inode.direntry.set_data('xattr', xa)

    def statfs(self, ctx):
        assert self._initialized
        raise llfuse.FUSEError(ENOSYS)

    def symlink(self, parent_inode, name, target, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, WX_OK, ctx), EPERM)
        raise llfuse.FUSEError(ENOSYS)

    def unlink(self, parent_inode, name, ctx, *, allow_any=False):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, WX_OK, ctx), EPERM)
        pn = self.forest.get_inode_by_value(parent_inode)
        n = self.forest.lookup(pn, name)
        assert_or_errno(n, ENOENT)
        try:
            assert_or_errno(n.leaf_node.is_file or allow_any, ENOENT)
            pn.node.remove(n.leaf_node)
            n.set_leaf_node(None)
        finally:
            n.deref()

    def write(self, fh, off, buf):
        assert self._initialized
        return self.forest.lookup_fd(fh).write(off, buf)
