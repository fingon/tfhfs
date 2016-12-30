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
# Last modified: Fri Dec 30 14:23:09 2016 mstenber
# Edit time:     441 min
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
import collections
import logging
import os
import stat
import errno
from errno import EEXIST, ENOATTR, ENOENT, ENOTEMPTY, EPERM
from ms.lazy import lazy_property
import pwd
import grp

import const
import forest_nodes
import llfuse
import inode

_debug = logging.getLogger(__name__).debug

WX_OK = os.W_OK | os.X_OK
RX_OK = os.R_OK | os.X_OK


def assert_or_errno(stmt, err, desc=None):
    if not stmt:
        _debug('result:%s(%d) %s', errno.errorcode.get(err, '???'), err, desc and desc or '')
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

    # Not-so-pretty stuff to deal with supplementary gids
    def _ctx_has_gid(self, ctx, gid):
        return gid == ctx.gid or gid in self._uid2supgids[ctx.uid]

    @lazy_property
    def _uid2supgids(self):
        d = collections.defaultdict(list)
        for uid in self._uid2usernames.keys():
            gids = []
            for username in self._uid2usernames[uid]:
                for gid in self._username2supgids[username]:
                    if gid in gids:
                        continue
                    gids.append(gid)
            d[uid] = gids
        return d

    @lazy_property
    def _uid2usernames(self):
        d = collections.defaultdict(list)
        for u in pwd.getpwall():
            d[u.pw_uid].append(u.pw_name)
        return d

    @lazy_property
    def _username2supgids(self):
        d = collections.defaultdict(list)
        for g in grp.getgrall():
            for u in g.gr_mem:
                d[u].append(g.gr_gid)
        return d

    def _ctx_sticky_mutate_check(self, ctx, pn, n):
        if pn.direntry.is_sticky:
            # own dir -> ok
            if ctx.uid == pn.direntry.data['st_uid']:
                return
            # own file -> ok
            if isinstance(n, inode.INode):
                n = n.direntry
            if ctx.uid == n.data['st_uid']:
                return
            assert_or_errno(not ctx.uid, EPERM, "sticky and not owner")

    def _leaf_attributes(self, leaf_node):
        entry = llfuse.EntryAttributes()
        entry.st_nlink = 1
        entry.generation = 0
        entry.entry_timeout = 5
        entry.attr_timeout = 5
        # 'st_nlink',  # fixed
        for attr in ('st_mode', 'st_uid', 'st_gid',
                     'st_rdev', 'st_size', 'st_atime_ns', 'st_mtime_ns',
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
        if self._ctx_has_gid(ctx, target_gid):
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
                        EPERM, 'access check for create')
        _debug('create @i:%s %s m%o f:0x%x %s',
               parent_inode, name, mode, flags, ctx)
        pn = self.forest.inodes.get_by_value(parent_inode)
        file_inode = self._lookup(pn, name, ctx)
        try:
            if file_inode:
                assert_or_errno(not (flags & os.O_EXCL), EEXIST,
                                'exists and O_EXCL')
                assert_or_errno(self.access(file_inode.value,
                                            _flags_to_perm(flags), ctx), EPERM,
                                'exists and no permissions')
            else:
                file_inode = self.forest.create_file(pn, name, mode=mode)
                self._set_de_perms_from_ctx(file_inode.direntry, ctx)
            fd = file_inode.open(flags)
            file_inode.changed_mtime()
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
        assert_or_errno(xa, ENOATTR, 'no xattr')
        v = xa.get(name)
        assert_or_errno(v, ENOATTR, 'no named xattr')
        return v

    def link(self, inode, new_parent_inode, new_name, ctx):
        assert self._initialized
        assert_or_errno(self.access(new_parent_inode, WX_OK, ctx), EPERM,
                        'no write access to new parent inode')
        inode = self.forest.inodes.get_by_value(inode)
        assert_or_errno(not inode.leaf_node, EEXIST,
                        'file already linked somewhere')
        pn = self.forest.inodes.get_by_value(new_parent_inode)
        n = self._lookup(pn, new_name, ctx, noref=True)
        assert_or_errno(not n, EEXIST, 'link with target name already exists')
        rn = pn.node
        leaf = rn.leaf_class(self.forest, name=new_name)
        rn.add_to_tree(leaf)
        inode.set_leaf_node(leaf)
        inode.changed2()
        # TBD: This clearly mutilates 'mode' (and other attributes)
        # quite severely as they are essentially default values. Is it
        # a problem?

    def listxattr(self, inode, ctx):
        assert self._initialized
        # assert_or_errno(self.access(inode, os.R_OK, ctx), EPERM)
        inode = self.forest.inodes.get_by_value(inode)
        return inode.direntry.data.get('xattr', {}).keys()

    def _lookup(self, parent_inode, name, ctx, *, noref=False):
        assert self._initialized
        if not isinstance(parent_inode, inode.INode):
            n = self.forest.inodes.get_by_value(parent_inode)
        else:
            n = parent_inode
        assert_or_errno(self.access(n.value, os.X_OK, ctx), EPERM,
                        'x perm missing from parent inode')
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
        # This path should be rare - so add+remove ref should not matter
        if n and noref:
            n.deref()
        return n

    def lookup(self, parent_inode, name, ctx):
        n = self._lookup(parent_inode, name, ctx)
        assert_or_errno(n, ENOENT, 'no entry in tree')
        return self._inode_attributes(n)

    def mkdir(self, parent_inode, name, mode, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, WX_OK, ctx), EPERM,
                        'wx perm missing from parent inode')
        inode = self.forest.inodes.get_by_value(parent_inode)
        cn = inode.node.search_name(name)
        assert_or_errno(not cn, EEXIST, 'node in tree')
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
        assert_or_errno(self.access(inode, _flags_to_perm(flags), ctx), EPERM,
                        'no perm')
        _debug('open i:%d f:0x%x %s', inode, flags, ctx)
        inode = self.forest.inodes.get_by_value(inode)
        if _flags_to_perm(flags) & os.W_OK:
            inode.changed_mtime()
        else:
            inode.changed_atime()
        return inode.open(flags)

    def opendir(self, inode, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, RX_OK, ctx), EPERM, 'no rx perm')
        inode = self.forest.inodes.get_by_value(inode)
        inode.changed_atime()
        inode.ref()
        return inode.value

    def read(self, fh, off, size):
        assert self._initialized
        fh = self.forest.fds.get_by_value(fh)
        # fh.inode.changed_atime()  # is this sane?
        return fh.read(off, size)

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
                        sbits = self.forest.inodes.max_value.bit_length() + 1
                        ino_bits = llfuse.get_ino_t_bits()
                        if sbits < ino_bits / 2:
                            sbits = ino_bits // 2
                        a.st_ino = (i << sbits) | dir_inode.value
                    assert a.st_ino  # otherwise not visible in e.g. ls!
                    yield t

    def readlink(self, inode, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, os.R_OK, ctx), EPERM, 'no r perm')
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
        assert_or_errno(self.access(inode, os.W_OK, ctx, or_own=True), EPERM,
                        'no w perm')
        inode = self.forest.inodes.get_by_value(inode)
        xa = inode.direntry.data.get('xattr', {})
        assert_or_errno(name in xa, ENOATTR, 'no such attr')
        del xa[name]
        inode.direntry.set_data('xattr', xa)

    def rename(self, parent_inode_old, name_old, parent_inode_new,
               name_new, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode_old, WX_OK, ctx),
                        EPERM, 'no wx perm to old parent')
        assert_or_errno(self.access(parent_inode_new, WX_OK, ctx),
                        EPERM, 'no wx perm to new parent')
        parent_inode_old = self.forest.inodes.get_by_value(parent_inode_old)
        parent_inode_new = self.forest.inodes.get_by_value(parent_inode_new)
        n = self._lookup(parent_inode_old, name_old, ctx)
        assert_or_errno(n, ENOENT, 'no entry in tree')
        self._ctx_sticky_mutate_check(ctx, parent_inode_old, n)
        self._ctx_sticky_mutate_check(ctx, parent_inode_new, n)
        n1 = self._lookup(parent_inode_new, name_new, ctx)
        # kill parent_inode_new / name_new first, if it exists
        if n1:
            if n1.direntry.is_dir:
                self.rmdir(parent_inode_new.value, name_new, ctx)
            else:
                self.unlink(parent_inode_new.value, name_new, ctx)
            n1.deref()
        old_data = n.direntry.data
        assert 'st_ino' not in old_data  # should never be persisted
        try:
            # remove old link
            self.unlink(parent_inode_old.value, name_old, ctx, allow_any=True)
            # add new link in new location
            self.link(n.value, parent_inode_new.value, name_new, ctx)
            n2 = self._lookup(parent_inode_new, name_new, ctx)
            assert n2
            n2.direntry.data.update(old_data)
            n2.deref()

        finally:
            n.deref()

    def rmdir(self, parent_inode, name, ctx):
        assert self._initialized
        assert_or_errno(self.access(parent_inode, WX_OK, ctx),
                        EPERM, 'no wx perm to parent inode')
        pn = self.forest.inodes.get_by_value(parent_inode)
        n = self._lookup(pn, name, ctx)
        assert_or_errno(n, ENOENT, 'no entry in tree')
        try:
            assert_or_errno(n.leaf_node.is_dir, ENOENT, 'non-directory target')
            self._ctx_sticky_mutate_check(ctx, pn, n)
            assert_or_errno(not n.node.children, ENOTEMPTY)
            n.leaf_node.root.remove_from_tree(n.leaf_node)
            pn.changed2()
            # TBD: Also check the subdir permission?
        finally:
            n.deref()

    def setattr(self, inode, attr, fields, fh, ctx):
        assert self._initialized
        inode = self.forest.inodes.get_by_value(inode)
        de = inode.direntry
        pending_changes = {}
        mode_filter = 0
        if fields.update_uid:
            now_st_uid = de.data['st_uid']
            if attr.st_uid >= 0 and attr.st_uid != now_st_uid:
                assert_or_errno(not ctx.uid, EPERM, 'nonroot setting uid')
                # Non-root can only do nop uid change
                pending_changes['st_uid'] = attr.st_uid

        if fields.update_gid:
            # Non-root can change gid to one he has, if he owns the file
            now_st_uid = de.data['st_uid']
            now_st_gid = de.data['st_gid']
            if attr.st_gid >= 0 and attr.st_gid != now_st_gid:
                assert_or_errno(not ctx.uid
                                or (ctx.uid == now_st_uid
                                    and self._ctx_has_gid(ctx, attr.st_gid)),
                                EPERM, 'nonroot setting gid')
                pending_changes['st_gid'] = attr.st_gid
                if ctx.uid:
                    mode_filter = stat.S_ISUID | stat.S_ISGID
                    # Non-root setuid/gid should reset setuid/gid
        if fields.update_mode:
            # TBD: use S_IFMT + S_IMODE to filter this or not?
            pending_changes['st_mode'] = attr.st_mode & ~mode_filter
        elif mode_filter:
            pending_changes['st_mode'] = de.data['st_mode'] & ~mode_filter

        if fields.update_mtime:
            _debug('%s mtime set to %s', inode, attr.st_mtime_ns)
            pending_changes['st_mtime_ns'] =  attr.st_mtime_ns
        if fields.update_size:
            pending_changes['st_size'] = attr.st_size
        pending_changes = {k:v for k, v in pending_changes.items()
                           if k not in de.data or de.data[k] != v}
        if pending_changes:
            for k, v in pending_changes.items():
                assert_or_errno(self.access(inode.value, os.W_OK, ctx,
                                            or_own=True),
                                EPERM, 'setattr-file write')
                if k == 'st_size':
                    inode.set_size(v)
                else:
                    de.set_data(k, v)
        inode.changed_ctime()
        return self._inode_attributes(inode)

    def setxattr(self, inode, name, value, ctx):
        assert self._initialized
        assert_or_errno(self.access(inode, os.W_OK, ctx,
                                    or_own=True), EPERM, 'no w perm')
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
        assert_or_errno(self.access(parent_inode, WX_OK, ctx),
                        EPERM, 'no wx perm to parent inode')
        n = self._lookup(parent_inode, name, ctx, noref=True)
        assert_or_errno(not n, EEXIST, 'link target exists')
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
        _debug('unlink %s %s %s', parent_inode, name, allow_any
               and "(any)" or "")
        assert_or_errno(self.access(parent_inode, WX_OK, ctx), EPERM,
                        'no wx perm to parent inode')
        pn = self.forest.inodes.get_by_value(parent_inode)
        n = self._lookup(pn, name, ctx)
        assert_or_errno(n, ENOENT)
        try:
            self._ctx_sticky_mutate_check(ctx, pn, n)
            if not allow_any:
                assert_or_errno(not n.leaf_node.is_dir, EPERM, 'dir target')
            pn.node.remove_from_tree(n.leaf_node)
            n.set_leaf_node(None)
            pn.changed2()
        finally:
            n.deref()

    def write(self, fh, off, buf):
        assert self._initialized
        return self.forest.fds.get_by_value(fh).write(off, buf)
