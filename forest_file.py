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
# Last modified: Fri Dec 30 10:14:12 2016 mstenber
# Edit time:     340 min
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

import concurrent.futures
import logging
import os
import struct

import const
import inode
import util
from forest_nodes import FileBlockEntry, FileBlockTreeNode, FileData

_debug = logging.getLogger(__name__).debug

KEY_STRUCT_FORMAT = '>Q'  # 64 bit long long


class FileDescriptor:
    inode = None

    def __init__(self, inode, flags):
        assert inode
        self.inode = inode
        self.flags = flags
        self.inode.ref()
        self.fds.register(self)

    def __del__(self):
        # MUST have been closed elsewhere!
        # assert self.inode is None

        # ^ This check is mostly spurious; normally objects are only
        # disappearing from fdstore via .close(). Only if whole
        # fdstore (=forest) terminates abruptly, they may disappear
        # 'faster'.
        pass

    def close(self):
        assert self.inode is not None
        self.fds.unregister(self)
        self.inode.deref()
        del self.inode

    def dup(self):
        fd2 = self.inode.open(self.flags & ~os.O_TRUNC)
        return fd2

    @property
    def fd(self):
        return self.fds.get_value_by_object(self)

    @property
    def fds(self):
        return self.forest.fds

    @property
    def forest(self):
        return self.inode.forest

    def flush(self):
        self.inode.flush()

    def read(self, ofs, size):
        return self.inode.read(ofs, size)

    def write(self, ofs, buf):
        if self.flags & os.O_APPEND:
            ofs = self.inode.size
        bufofs = self.inode.write(ofs, buf)
        return bufofs


def _maybe_threaded_map(fun, l):
    l = list(l)
    if len(l) > 2:
        with concurrent.futures.ThreadPoolExecutor() as e:
            return e.map(fun, l)
    return map(fun, l)


class FileINode(util.DirtyMixin, inode.INode):

    @property
    def is_minifile(self):
        return self.leaf_node and self.leaf_node.data.get('minifile')

    def load_node(self):
        if self.node is None:
            ln = self.leaf_node
            if ln:
                bid = ln.block_id
                if bid:
                    if self.is_minifile:
                        self.set_node(FileData(self.forest, bid, None))
                    else:
                        self.set_node(FileBlockTreeNode(self.forest, bid))
        return self.node

    def mark_dirty_related(self):
        self.forest.dirty_file_set.add(self)

    def open(self, flags):
        if flags & os.O_TRUNC:
            self.set_size(0)
        o = FileDescriptor(self, flags)
        return o.fd

    def perform_flush(self):
        if not self._write_blocks:
            return
        _debug('%s perform_flush (%d blocks))', self, len(self._write_blocks))

        def _rewrite_pending_one(t):
            k, (cn, s) = t
            _debug(' %s = %d bytes', k, len(s))
            # If the child node does not exist, create it and add to tree
            if not cn:
                cn = FileBlockEntry(self.forest, name=k)
                self.node.add_to_tree(cn)
            return cn, s

        def _store_entry(t):
            (cn, s) = t
            bid = self.forest.refer_or_store_block_by_data(s)
            cn.set_block_id(bid)
            self.forest.storage.release_block(bid)

        pending_stores = [_rewrite_pending_one(t)
                          for t in self._write_blocks.items()]
        list(_maybe_threaded_map(_store_entry, pending_stores))
        del self._write_blocks

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
            r = util.zeropad_bytes(size, r)
        _debug('read %d bytes from ofs %d/%d %s',
               len(r), oofs, ofs, pad and '(pad)' or '')
        return r

    def set_size(self, size, minimum_size=None):
        if self.size == size:
            return
        if minimum_size is None:
            self.flush()
        if size > const.BLOCK_SIZE_LIMIT:
            # Should be node tree, or convert to one
            self._to_block_tree(size, minimum_size)
        elif size > const.INTERNED_BLOCK_DATA_SIZE_LIMIT or not self.leaf_node:
            self._to_block_data(size)
        else:
            self._to_interned_data(size)
        if self.leaf_node:
            self.direntry.set_data('st_size', size)
        _debug('set size to %d', self.size)
        assert self.size == size

    _write_blocks = None

    def set_write_block(self, node, name, data):
        if self._write_blocks is None:
            self._write_blocks = {}
        _debug('stored %d bytes for %s', len(data), name)
        self._write_blocks[name] = [node, data]

    @property
    def size(self):
        if self.leaf_node:
            return self.direntry.data.get('st_size', 0)
        return self.stored_size

    @property
    def stored_size(self):
        self.flush()
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
        (ofs,) = struct.unpack(KEY_STRUCT_FORMAT, ll.key)
        r = (ofs * const.BLOCK_SIZE_LIMIT) + len(ll.content)
        _debug('size full %d, partial %s=%d => %d',
               ofs, ll.key, len(ll.content), r)
        return r

    def write(self, ofs, buf):
        size = len(buf)
        nsize = ofs + size
        if self.size < nsize:
            self.set_size(nsize, minimum_size=ofs)
        done = 0
        while done < len(buf):
            wrote = self._write(ofs + done, buf, done)
            done += wrote
        _debug('wrote %d total to %d', done, ofs)
        self.changed_mtime()
        return done

    def _write(self, ofs, buf, bufofs=0):
        _debug('_write @%d %s(%d+)', ofs, len(buf), bufofs)
        obufofs = bufofs
        size = len(buf)

        def _replace(s, ofs, buf, bufofs, bufmax):
            if s is None:
                s = b''
            assert isinstance(s, bytes)
            bufmax = bufmax or len(buf)
            ns = buf[bufofs:bufofs + bufmax]
            rofs = ofs + len(ns)
            _debug('_replace %d: %d/%d = %d', len(s), ofs, rofs, len(ns))
            topad = max(0, ofs - len(s))
            rs = b'%s%s%s%s' % (s[:ofs], bytes(topad), ns, s[rofs:])
            ofs += len(ns)
            bufofs += len(ns)
            _debug(' => %d', len(rs))
            return rs, bufofs

        ln = self.leaf_node
        n = self.load_node()
        if n is None:
            s, bufofs = _replace(ln.block_data, ofs, buf, bufofs, 0)
            ln.set_block_data(s)
        elif isinstance(n, FileData):
            s, bufofs = _replace(n.content, ofs, buf, bufofs, 0)
            self.set_node(FileData(self.forest, None, s))
        else:
            _debug(' tree @%d %d', ofs, size)
            cn, k, d, ofs = self._tree_node_key_data_for_ofs(ofs, size)
            s, bufofs = _replace(d, ofs, buf, bufofs,
                                 min(size, const.BLOCK_SIZE_LIMIT - ofs))
            self.mark_dirty()
            self.set_write_block(node=cn, name=k, data=s)
        wrote = bufofs - obufofs
        _debug(' = %d bytes written (result len %d)', wrote, len(s))
        return wrote

    def _to_block_tree(self, size, minimum_size=None):
        _debug('_to_block_tree %d (%s)', size, minimum_size)
        ln = self.leaf_node
        if not isinstance(self.load_node(), FileBlockTreeNode):
            ln.set_data('minifile', None)
            # If we are not already tree, convert to tree + write data in
            had_size = self.size
            buf = self.read(0, had_size)
            ln.set_block_data(None)
            n = FileBlockTreeNode(self.forest)
            n._loaded = True
            self.set_node(n)
            self._write(0, buf)  # no resize
            ssize = self.stored_size
        else:
            ssize = self.size
        if ssize > size:
            self.flush()
            # Grab bytes from the last relevant ofs
            bofs = size - size % const.BLOCK_SIZE_LIMIT
            buf = self.read(bofs, size % const.BLOCK_SIZE_LIMIT)

            # Kill all blocks, including the last valid one
            while True:
                try:
                    ll = self.node.last_leaf
                except IndexError:
                    break
                (ofs,) = struct.unpack(KEY_STRUCT_FORMAT, ll.key)
                ofs = ofs * const.BLOCK_SIZE_LIMIT
                if ofs >= size:
                    self.node.remove_from_tree(ll)
                else:
                    break
            if buf:
                self._write(bofs, buf)
            self.flush()
            ssize = self.stored_size
        # If we know we are planning to write the bytes in
        # [minimum_size, size[ range, no need to care about writing to
        # tree here.
        if minimum_size is not None:
            size = minimum_size
        if ssize < size:
            self._write(size, b'')

    def _to_block_data(self, size):
        _debug('_to_block_data %d', size)
        s = self._read(0, size, pad=True)
        ln = self.leaf_node
        if ln:
            ln.set_data('minifile', True)
            ln.set_block_data(None)
        self.set_node(FileData(self.forest, None, s))
        if self._write_blocks:
            del self._write_blocks

    def _to_interned_data(self, size):
        _debug('_to_interned_data %d', size)
        s = self._read(0, size, pad=True)
        ln = self.leaf_node
        ln.set_data('minifile', None)
        ln.set_block_data(s)
        self.set_node(None)
        if self._write_blocks:
            del self._write_blocks

    def _tree_node_key_data_for_ofs(self, ofs, size):
        n = self.load_node()
        assert isinstance(n, FileBlockTreeNode)
        k, ofs, size = self._tree_key_for_ofs(ofs, size)
        t = (self._write_blocks or {}).get(k)
        if t:
            n, d = t
        else:
            n = n.search_name(k)
            if n:
                d = n.content
            else:
                # Non-last nodes are implicitly all zeroes
                d = bytes(size)
        return n, k, d, ofs

    def _tree_key_for_ofs(self, ofs, size):
        kn = ofs // const.BLOCK_SIZE_LIMIT
        ofs -= kn * const.BLOCK_SIZE_LIMIT
        ofs = int(ofs)
        _debug(' => block#%d ofs#%d', kn, ofs)
        k = struct.pack(KEY_STRUCT_FORMAT, int(kn))
        size = min(size, const.BLOCK_SIZE_LIMIT - ofs)
        _debug(' => size %d', size)
        return k, ofs, size

    @property
    def _leaf_interned_block_data(self):
        return self.leaf_node and self.leaf_node.block_data

    def set_leaf_node(self, node):
        if node is None and self.leaf_node and self._leaf_interned_block_data:
            self._to_block_data(self.size)
        inode.INode.set_leaf_node(self, node)
