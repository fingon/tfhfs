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
# Last modified: Sat Dec 24 06:38:49 2016 mstenber
# Edit time:     323 min
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
from errno import EEXIST, ENOATTR, ENOENT, ENOTEMPTY, EPERM

import const
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
        entry.st_nlink = 1
        entry.generation = 0
        entry.entry_timeout = 5
        entry.attr_timeout = 5
        # 'st_nlink',  # fixed
        # 'st_atime_ns',  # never provided
        for attr in ('st_mode', 'st_uid', 'st_gid',
                     'st_rdev', 'st_size', 'st_mtime_ns',
                     'st_ctime_ns'):
            v = leaf_node.data.get(attr)
            if v is not None:
                _debug('%s = %s', attr, v)
                setattr(entry, attr, v)
        entry.st_blksize = const.BLOCK_SIZE_LIMIT
        entry.st_blocks = max(0, (entry.st_size - 1)) // entry.st_blksize + 1
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
        return self

    def destroy(self):
        assert self._initialized

        # Store current state to disk, and then restart from scratch.
        # Running forget-equivalents does not seem appealing.
        self.forest.flush()
        self.forest.init()

        del self._initialized

    # Normal client API

    def _de_access(self, de, mode, ctx, *, or_own=False):
        dedata = de.data
        target_mode = dedata.get('st_mode', 0)
        target_uid = dedata.get('st_uid', 0)
        target_gid = dedata.get('st_gid', 0)
        perms = target_mode & 0x7
        if ctx.gid == target_gid:
            perms |= (target_mode >> 3) & 0x7
        if ctx.uid == target_uid:
            perms |= (target_mode >> 6) & 0x7
        r = ((perms & mode) == mode
             or not ctx.uid or (ctx.uid == target_uid and or_own))
        _debug('uid:%d perms:%d, target:%d => %s', ctx.uid, perms, mode, r)
        return r

    def access(self, inode, mode, ctx, **kw):
        assert self._initialized
        assert mode  # F_OK = 0, but should not show up(?)
        inode = self.forest.inodes.get_by_value(inode)
        return self._de_access(inode.direntry, mode, ctx, **kw)

    def _set_de_perms_from_ctx(self, de, ctx):
        de.set_data('st_uid', ctx.uid)
        de.set_data('st_gid', ctx.gid)
        de.set_data('st_mode', de.mode & ~ctx.umask)

    def create(self, parent_inode, name, mode, flags, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, _flags_to_perm(flags), ctx),
                        EPERM)
        _debug('create @i:%s %s m%o f:0x%x %s',
               parent_inode, name, mode, flags, ctx)
        n = self.forest.inodes.get_by_value(parent_inode)
        try:
            file_inode = self.forest.lookup(n, name)
            if file_inode:
                assert_or_errno(not (flags & os.O_EXCL), EEXIST)
                assert_or_errno(self.access(file_inode.value,
                                            _flags_to_perm(flags), ctx), EPERM)
            else:
                file_inode = self.forest.create_file(n, name, mode=mode)
                self._set_de_perms_from_ctx(file_inode.direntry, ctx)
            fd = file_inode.open(flags)
        except:
            file_inode.deref()
            raise
        # We can return it and increment the refcnt implicitly (done
        # in lookup/create_file)
        return fd, self._inode_attributes(file_inode)

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
        self.forest.inodes.get_by_value(inode).deref(count)

    def fsync(self, fh, datasync):
        assert self._initialized
        self.forest.fds.get_by_value(fh).flush()

    def fsyncdir(self, fh, datasync):
        assert self._initialized
        self.forest.inodes.get_by_value(fh).flush()

    def getattr(self, inode, ctx):
        assert self._initialized
        # assert_or_errno(self.access(inode, os.R_OK, ctx, or_own=True), EPERM)
        return self._inode_attributes(self.forest.inodes.get_by_value(inode))

    def getxattr(self, inode, name, ctx):
        assert self._initialized
        # assert_or_errno(self.access(inode, os.R_OK, ctx, or_own=True), EPERM)
        inode = self.forest.inodes.get_by_value(inode)
        xa = inode.direntry.data.get('xattr')
        assert_or_errno(xa, ENOATTR)
        v = xa.get(name)
        assert_or_errno(v, ENOATTR)
        return v

    def link(self, inode, new_parent_inode, new_name, ctx):
        assert self._initialized
        assert_or_errno(self.access(new_parent_inode, WX_OK, ctx), EPERM)
        inode = self.forest.inodes.get_by_value(inode)
        assert_or_errno(not inode.leaf_node, EEXIST)
        parent_inode = self.forest.inodes.get_by_value(new_parent_inode)
        n = parent_inode.node.search_name(new_name)
        if n:
            self.unlink(new_parent_inode, new_name, ctx)
        rn = parent_inode.node
        leaf = rn.leaf_class(self.forest, name=new_name)
        self.forest.inodes.get_by_node(rn).set_node(rn.add(leaf))
        inode.set_leaf_node(leaf)
        # TBD: This clearly mutilates 'mode' (and other attributes)
        # quite severely as they are essentially default values. Is it
        # a problem?

    def listxattr(self, inode, ctx):
        assert self._initialized
        # assert_or_errno(self.access(inode, os.R_OK, ctx), EPERM)
        inode = self.forest.inodes.get_by_value(inode)
        return inode.direntry.data.get('xattr', {}).keys()

    def lookup(self, parent_inode, name, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, os.X_OK, ctx), EPERM)
        n = self.forest.inodes.get_by_value(parent_inode)
        if name == b'.':
            n.ref()
        elif name == b'..':
            cn = n.leaf_node
            if cn:
                n = self.forest.inodes.get_by_node(cn.root).ref()
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
        inode = self.forest.inodes.get_by_value(parent_inode)
        cn = inode.node.search_name(name)
        assert_or_errno(not cn, EEXIST)
        dir_inode = self.forest.create_dir(inode, name=name,
                                           mode=(mode & ~ctx.umask))
        self._set_de_perms_from_ctx(dir_inode.direntry, ctx)
        return self._inode_attributes(dir_inode)

    def mknod(self, parent_inode, name, mode, rdev, ctx):
        assert self._initialized
        fd, a = self.create(parent_inode, name, mode,
                            os.O_TRUNC | os.O_CREAT, ctx)
        inode = self.forest.inodes.get_by_value(a.st_ino)
        if stat.S_ISCHR(mode) or stat.S_ISBLK(mode):
            inode.direntry.set_data('st_rdev', rdev)
        self.release(fd)
        return self._inode_attributes(inode)

    def open(self, inode, flags, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, _flags_to_perm(flags), ctx), EPERM)
        _debug('open i:%d f:0x%x %s', inode, flags, ctx)
        return self.forest.inodes.get_by_value(inode).open(flags)

    def opendir(self, inode, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, RX_OK, ctx), EPERM)
        inode = self.forest.inodes.get_by_value(inode)
        inode.ref()
        return inode.value

    def read(self, fh, off, size):
        assert self._initialized
        return self.forest.fds.get_by_value(fh).read(off, size)

    def readdir(self, fh, off):
        assert self._initialized
        dir_inode = self.forest.inodes.get_by_value(fh)
        pln = None
        for i, ln in enumerate(dir_inode.node.get_leaves(), 1):
            # Additions may screw up the tree bit
            if (pln is not None and pln.key > ln.key) or not ln.name:
                pass
            else:
                pln = ln
                if i > off:
                    a = self._leaf_attributes(ln)
                    t = (ln.name, a, i)
                    # If we already have inode for this, we can use it.
                    # If not, we synthesize one that is highly unique
                    # but not really usable anywhere elsewhere (sigh)
                    inode = self.forest.inodes.getdefault_by_leaf_node(ln)
                    if inode:
                        a.st_ino = inode.value
                    else:
                        sbits = self.forest.inodes.max_value + 1
                        a.st_ino = (i << sbits) | dir_inode.value
                    assert a.st_ino  # otherwise not visible in e.g. ls!
                    yield t

    def readlink(self, inode, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, os.R_OK, ctx), EPERM)
        inode = self.forest.inodes.get_by_value(inode)
        fd = self.open(inode.value, os.O_RDONLY, ctx)
        try:
            return self.read(fd, 0, inode.size)
        finally:
            self.release(fd)

    def release(self, fh):
        assert self._initialized
        self.forest.fds.get_by_value(fh).close()

    def releasedir(self, fh):
        assert self._initialized
        self.forest.inodes.get_by_value(fh).deref()

    def removexattr(self, inode, name, ctx):
        assert_or_errno(self.access(inode, os.W_OK, ctx, or_own=True), EPERM)
        inode = self.forest.inodes.get_by_value(inode)
        xa = inode.direntry.data.get('xattr', {})
        assert_or_errno(name in xa, ENOATTR)
        del xa[name]
        inode.direntry.set_data('xattr', xa)

    def rename(self, parent_inode_old, name_old, parent_inode_new,
               name_new, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode_old, WX_OK, ctx), EPERM)
        assert_or_errno(self.access(parent_inode_new, WX_OK, ctx), EPERM)
        parent_inode_old = self.forest.inodes.get_by_value(parent_inode_old)
        parent_inode_new = self.forest.inodes.get_by_value(parent_inode_new)
        n = self.forest.lookup(parent_inode_old, name_old)
        assert_or_errno(n, ENOENT)
        old_data = n.direntry.data
        assert 'st_ino' not in old_data  # should never be persisted
        try:
            self.unlink(parent_inode_old.value, name_old, ctx, allow_any=True)
            self.link(n.value, parent_inode_new.value, name_new, ctx)
            n2 = self.forest.lookup(parent_inode_new, name_new)
            assert n2
            n2.direntry.data.update(old_data)
            n2.deref()

        finally:
            n.deref()

    def rmdir(self, parent_inode, name, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, WX_OK, ctx), EPERM)
        pn = self.forest.inodes.get_by_value(parent_inode)
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
        inode = self.forest.inodes.get_by_value(inode)
        de = inode.direntry
        assert_or_errno(self.access(inode.value, os.W_OK, ctx, or_own=True),
                        EPERM)
        if fields.update_uid:
            assert_or_errno(not ctx.uid, EPERM)
            de.set_data('st_uid', attr.st_uid)
        if fields.update_gid:
            assert_or_errno(not ctx.uid, EPERM)
            de.set_data('st_gid', attr.st_gid)
        if fields.update_mtime:
            de.set_data('st_mtime_ns', attr.st_mtime_ns)
        if fields.update_mode:
            # TBD: use S_IFMT + S_IMODE to filter this or not?
            de.set_data('st_mode', attr.st_mode)
        if fields.update_size:
            inode.set_size(attr.st_size)
        return self._inode_attributes(inode)

    def setxattr(self, inode, name, value, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, os.W_OK, ctx, or_own=True), EPERM)
        inode = self.forest.inodes.get_by_value(inode)
        xa = inode.direntry.data.get('xattr', {})
        xa[name] = value
        inode.direntry.set_data('xattr', xa)

    def statfs(self, ctx):
        assert self._initialized
        d = llfuse.StatvfsData()
        # from man page:
        # fsblkcnt_t     f_bavail;   /* # free blocks for unprivileged users */
        # fsblkcnt_t     f_bfree;    /* # free blocks */
        # fsblkcnt_t     f_blocks;   /* size of fs in f_frsize units */
        # unsigned long  f_bsize;    /* file system block size */
        # fsfilcnt_t     f_favail;   /* # free inodes for unprivileged users */
        # fsfilcnt_t     f_ffree;    /* # free inodes */
        # fsfilcnt_t     f_files;    /* # inodes */
        # unsigned long  f_frsize;   /* fragment size */

        # these are so n/a it is not even funny
        # f_ffree
        # f_files

        # constants
        d.f_bsize = const.BLOCK_SIZE_LIMIT
        d.f_frsize = const.BLOCK_SIZE_LIMIT

        avail = self.forest.storage.get_bytes_available()
        used = self.forest.storage.get_bytes_used()

        # return st.f_bavail * st.f_frsize / 1024 / 1024
        d.f_bfree = avail / d.f_frsize
        d.f_blocks = (avail + used) / d.f_frsize

        # unpriviliged have all resources
        d.f_bavail = d.f_bfree
        d.f_favail = d.f_ffree

        return d

    def symlink(self, parent_inode, name, target, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, WX_OK, ctx), EPERM)
        try:
            self.unlink(parent_inode, name, ctx)
        except llfuse.FUSEError as e:
            if e.errno != ENOENT:
                raise
        fd, a = self.create(parent_inode, name, stat.S_IFLNK | 0o777,
                            os.O_WRONLY, ctx)
        try:
            self.write(fd, 0, target)
            return a
        finally:
            self.release(fd)

    def unlink(self, parent_inode, name, ctx, *, allow_any=False):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, WX_OK, ctx), EPERM)
        pn = self.forest.inodes.get_by_value(parent_inode)
        n = self.forest.lookup(pn, name)
        assert_or_errno(n, ENOENT)
        try:
            assert_or_errno(self._de_access(
                n.direntry, os.W_OK, ctx, or_own=True), EPERM)
            assert_or_errno(n.leaf_node.is_file or allow_any, ENOENT)
            pn.node.remove(n.leaf_node)
            n.set_leaf_node(None)
        finally:
            n.deref()

    def write(self, fh, off, buf):
        assert self._initialized
        return self.forest.fds.get_by_value(fh).write(off, buf)
