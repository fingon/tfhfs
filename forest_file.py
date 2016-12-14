#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: forest_file.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Dec  3 17:50:30 2016 mstenber
# Last modified: Thu Dec 15 07:20:52 2016 mstenber
# Edit time:     210 min
#
"""This is the file abstraction which is an INode subclass.

It wraps the lifecycle of a node in terms of different sizes;

- the minimal one with simply small amount of embedded data,

- single-block medium sized file, and

- large file which consists of a tree of data blocks.

The file inodes transition about these different size modes
transparently to the user.

The 'leaf' node (that is leaf node in the supertree) is where the file
is anchored. The content reading is done by accessing the existing
leaf (small file), raw data (medium file), or the tree (large
file). Writes cause transition between tree modes.

Growth:
 small -> medium: clear block_data, refer to explicit block with the data
 medium -> large: add tree with the medium block as its first (and only) block.

Shrinking:
 large -> medium: if only one block left, collapse the tree
 medium -> small: small remove block_id, set block_data

"""

import logging
import os
import struct

import const
import inode
from forest_nodes import FileBlockEntry, FileBlockTreeNode, FileData

_debug = logging.getLogger(__name__).debug


def _zeropad(size, s=b''):
    topad = size - len(s)
    if topad > 0:
        s = s + bytes([0] * topad)
    return s


class FDStore:

    def __init__(self):
        self.fd2o = {}
        self.freefds = []

    def allocate_fd(self):
        if self.freefds:
            fd = self.freefds.pop(0)
            _debug('reused free fd #%d', fd)
        else:
            fd = len(self.fd2o) + 1
            _debug('allocated new fd #%d', fd)
        assert fd not in self.fd2o
        self.fd2o[fd] = None
        return fd

    def lookup_fd(self, fd):
        return self.fd2o[fd]

    def register_fd(self, fd, o):
        assert o
        assert self.fd2o[fd] is None
        self.fd2o[fd] = o
        _debug('register_fd #%d = %s', fd, o)

    def unregister_fd(self, fd):
        _debug('unregister_fd #%d', fd)
        self.freefds.append(fd)
        del self.fd2o[fd]


class FileDescriptor:
    inode = None

    def __init__(self, fd, inode, flags):
        assert inode
        self.fd = fd
        self.inode = inode
        self.flags = flags
        self.inode.ref()

    def __del__(self):
        self.close()

    def close(self):
        if self.inode is None:
            return
        self.inode.store.unregister_fd(self.fd)
        self.inode.deref()
        del self.inode

    def dup(self):
        fd2 = self.inode.open(self.flags & ~os.O_TRUNC)
        return fd2

    def flush(self):
        self.inode.flush()

    def read(self, ofs, size):
        return self.inode.read(ofs, size)

    def write(self, ofs, buf):
        if self.flags & os.O_APPEND:
            ofs = self.inode.size
        bufofs = self.inode.write(ofs, buf)
        return bufofs


class FileINode(inode.INode):

    def load_node(self):
        if self.node is None:
            ln = self.leaf_node
            bid = ln.block_id
            if bid:
                if ln.mode & const.DENTRY_MODE_MINIFILE:
                    self.set_node(FileData(self.store, bid, None))
                else:
                    self.set_node(FileBlockTreeNode(self.store, bid))
        return self.node

    def open(self, flags):
        assert isinstance(self.store, FDStore)
        if flags & os.O_TRUNC:
            self.set_size(0)
        fd = self.store.allocate_fd()
        o = FileDescriptor(fd, self, flags)
        self.store.register_fd(fd, o)
        return fd

    def read(self, ofs, size):
        assert ofs >= 0
        assert size >= 0
        l = []
        while size > 0:
            r = self._read(ofs, size)
            if not r:
                break
            l.append(r)
            size -= len(r)
            ofs += len(r)
        return b''.join(l)

    def _read(self, ofs, size, *, pad=False):
        oofs = ofs
        d = None
        n = self.load_node()
        if n is None:
            ln = self.leaf_node
            if ln.block_data:
                d = ln.block_data
        elif isinstance(n, FileData):
            d = n.content
        else:
            assert isinstance(n, FileBlockTreeNode)
            n, _, d, ofs = self._tree_node_key_data_for_ofs(ofs, size)
            pad = True
            size = int(min(size, self.size - oofs,
                           const.BLOCK_SIZE_LIMIT - ofs))
        r = d and d[ofs:ofs + size] or b''
        if pad:
            r = _zeropad(size, r)
        _debug('read %d bytes from ofs %d/%d %s',
               len(r), oofs, ofs, pad and '(pad)' or '')
        return r

    def set_size(self, size):
        if self.size == size:
            return
        if size > const.BLOCK_SIZE_LIMIT:
            # Should be node tree, or convert to one
            self._to_block_tree(size)
        elif size > const.INTERNED_BLOCK_DATA_SIZE_LIMIT:
            self._to_block_data(size)
        else:
            self._to_interned_data(size)
        _debug('set size to %d', self.size)
        assert self.size == size

    @property
    def size(self):
        n = self.load_node()
        if n is None:
            bd = self.leaf_node.block_data or b''
            return len(bd)
        if isinstance(n, FileData):
            return len(n.content)
        try:
            ll = n.last_leaf
        except IndexError:
            return 0
        (ofs,) = struct.unpack('>I', ll.key)
        r = (ofs * const.BLOCK_SIZE_LIMIT) + len(ll.content)
        _debug('size full %d, partial %s=%d => %d',
               ofs, ll.key, len(ll.content), r)
        return r

    def write(self, ofs, buf):
        size = len(buf)
        nsize = ofs + size
        if self.size < nsize:
            self.set_size(nsize)
        done = 0
        while done < len(buf):
            wrote = self._write(ofs + done, buf[done:])
            done += wrote
        _debug('wrote %d total to %d', done, ofs)
        return done

    def _write(self, ofs, buf):
        _debug('_write @%d %s', ofs, len(buf))
        size = len(buf)

        def _replace(s, ofs, buf, bufofs, bufmax):
            if s is None:
                s = b''
            assert isinstance(s, bytes)
            bufmax = bufmax or len(buf)
            ns = buf[bufofs:bufofs + bufmax]
            rofs = ofs + len(ns)
            _debug('_replace %d: %d/%d = %d', len(s), ofs, rofs, len(ns))
            rs = _zeropad(ofs, s[:ofs]) + ns + s[rofs:]
            ofs += len(ns)
            bufofs += len(ns)
            _debug(' => %d', len(rs))
            return rs, bufofs

        ln = self.leaf_node
        n = self.load_node()
        if n is None:
            s, bufofs = _replace(ln.block_data, ofs, buf, 0, 0)
            ln.set_block_data(s)
        elif isinstance(n, FileData):
            s, bufofs = _replace(n.content, ofs, buf, 0, 0)
            self.set_node(FileData(self.store, None, s))
        else:
            _debug(' tree @%d %d', ofs, size)
            cn, k, d, ofs = self._tree_node_key_data_for_ofs(ofs, size)
            s, bufofs = _replace(d, ofs, buf, 0,
                                 min(size, const.BLOCK_SIZE_LIMIT - ofs))
            # If the child node does not exist, create it and add to tree
            if not cn:
                cn = FileBlockEntry(self.store, name=k)
                n.add_child(cn)
            bid = self.store.refer_or_store_block_by_data(s)
            cn.set_block_id(bid)
            self.store.storage.release_block(bid)
        _debug(' = %d bytes written (result len %d)', bufofs, len(s))
        return bufofs

    def _to_block_tree(self, size):
        _debug('_to_block_tree %d', size)
        ln = self.leaf_node
        if not isinstance(self.load_node(), FileBlockTreeNode):
            ln.set_clear_mode_bits(0, const.DENTRY_MODE_MINIFILE)
            # If we are not already tree, convert to tree + write data in
            had_size = self.size
            buf = self.read(0, had_size)
            ln.set_block_data(None)
            n = FileBlockTreeNode(self.store)
            n._loaded = True
            self.set_node(n)
            self._write(0, buf)  # no resize
        self._write(size, b'')

    def _to_block_data(self, size):
        _debug('_to_block_data %d', size)
        s = self._read(0, size, pad=True)
        ln = self.leaf_node
        ln.set_clear_mode_bits(const.DENTRY_MODE_MINIFILE, 0)
        ln.set_block_data(None)
        self.set_node(FileData(self.store, None, s))

    def _to_interned_data(self, size):
        _debug('_to_interned_data %d', size)
        s = self._read(0, size, pad=True)
        ln = self.leaf_node
        ln.set_block_id(None)
        ln.set_block_data(s)

    def _tree_node_key_data_for_ofs(self, ofs, size):
        n = self.load_node()
        assert isinstance(n, FileBlockTreeNode)
        k, ofs, size = self._tree_key_for_ofs(ofs, size)
        n = n.search_name(k)
        if n:
            d = n.content
        else:
            # Non-last nodes are implicitly all zeroes
            d = _zeropad(size)
        return n, k, d, ofs

    def _tree_key_for_ofs(self, ofs, size):
        kn = ofs // const.BLOCK_SIZE_LIMIT
        ofs -= kn * const.BLOCK_SIZE_LIMIT
        ofs = int(ofs)
        _debug(' => block#%d ofs#%d', kn, ofs)
        k = struct.pack('>I', int(kn))
        size = min(size, const.BLOCK_SIZE_LIMIT - ofs)
        _debug(' => size %d', size)
        return k, ofs, size
