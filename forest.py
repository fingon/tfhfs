#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: forest.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Thu Jun 30 14:25:38 2016 mstenber
# Last modified: Tue Jul  5 15:40:38 2016 mstenber
# Edit time:     233 min
#
"""This is the 'forest layer' main module.

It implements nested tree concept, with an interface to the storage
layer.

While on-disk snapshot is always static, more recent, in-memory one is
definitely not. Flushed-to-disk parts of the tree m ay be eventually
purged as desired.

"""

import collections
import hashlib
import logging

import cbor

import btree
import const

_debug = logging.getLogger(__name__).debug


class DirtyMixin:
    dirty = False

    def mark_dirty(self):
        if self.dirty:
            return
        _debug('marked dirty: %s', self)
        self.dirty = True
        self.mark_dirty_related()
        return True

    def mark_dirty_related(self):
        raise NotImplementedError

    def flush(self):
        if not self.dirty:
            return
        del self.dirty
        return self.perform_flush()

    def perform_flush(self):
        raise NotImplementedError


def _sha256(*l):
    h = hashlib.sha256()
    for s in l:
        if isinstance(s, int):
            s = bytes([s])
        h.update(s)
    return h.digest()


class DataMixin(DirtyMixin):
    _data = None

    @property
    def data(self):
        if self._data is None:
            self._data = {}
        return self._data

    def mark_dirty_related(self):
        if self.parent:
            self.parent.mark_dirty()

    def set_data(self, k, v):
        if self._data == None:
            self._data = {}
        if self._data.get(k) is not v:
            self._data[k] = v
            self.mark_dirty()


class LoadedTreeNode(DataMixin, btree.TreeNode):
    _loaded = False

    def __init__(self, forest, block_id=None):
        self._forest = forest
        self._block_id = block_id
        btree.TreeNode.__init__(self)

    @property
    def child_keys(self):
        if not self._loaded:
            self.load()
            assert self._loaded
        return self._child_keys

    @property
    def children(self):
        if not self._loaded:
            self.load()
            assert self._loaded
        return self._children

    def create(self):
        return self.__class__(self._forest)

    @property
    def is_loaded(self):
        return self._loaded

    def load(self, block_id=None):
        assert not self._loaded
        if block_id is not None:
            self._block_id = block_id
        data = self._forest.storage.get_block_data_by_id(self._block_id)
        if data is not None:
            self.load_from_data(data)
            assert self._loaded
        else:
            # otherwise we are already empty node
            self._loaded = True
        return self

    def load_from_data(self, d):
        self._loaded = True
        (t, d) = d
        assert t & const.TYPE_MASK == self.entry_type
        (self.key, self._data, child_data_list) = cbor.loads(d)
        for cd in child_data_list:
            if t & const.BIT_LEAFY:
                cls2 = self.leaf_class
            else:
                cls2 = self.__class__
            tn2 = cls2(self._forest).load_interned_data(cd)
            self._add_child(tn2, skip_dirty=True)
        assert len(self._child_keys) == len(self._children)
        return self

    def load_interned_data(self, d):
        (self.key, self._block_id, self._data) = cbor.loads(d)
        return self

    def mark_dirty_related(self):
        super(LoadedTreeNode, self).mark_dirty_related()
        if self.parent is None:
            self._forest.dirty_node_set.add(self)

    def perform_flush(self):
        if not self.is_loaded:
            return
        self.dirty = False
        for child in self.children:
            child.flush()
        data = self.to_data()
        block_id = _sha256(*data)
        if block_id != self._block_id:
            self._forest.storage.refer_or_store_block(block_id, data)
            if self._block_id is not None:
                self._forest.storage.release_block(self._block_id)
            self._block_id = block_id
            return True

    def to_data(self):
        # n/a: 'key' (should be known already)
        l = [self.key, self.data,
             [x.to_interned_data() for x in self.children]]
        t = self.entry_type
        if self.is_leafy:
            t = t | const.BIT_LEAFY
        return (t, cbor.dumps(l))

    def to_interned_data(self):
        return cbor.dumps((self.key, self._block_id, self._data))


class DirectoryEntry(DataMixin, btree.LeafNode):

    _inode = None  # of the child that we represent

    def __init__(self, forest):
        self._forest = forest
        self._block_id = None
        btree.LeafNode.__init__(self)

    def perform_flush(self):
        if self._inode is not None:
            c = self._forest.get_dir_inode(self._inode)
            if c:
                c.flush()
                assert c._block_id
                if self._block_id != c._block_id:
                    if self._block_id:
                        self._forest.storage.release_block(self._block_id)
                    self._block_id = c._block_id
                    self._forest.storage.refer_block(c._block_id)
        return True

    def set_inode(self, inode):
        """ For the run-time, mark that this DirectoryEntry has a whole subtree which is determined by particular inode. """
        if self._inode == inode:
            return
        if self._inode and self._inode in self._forest.inode2deps:
            self._forest.inode2deps[self._inode].discard(self)
        self._inode = inode
        if inode:
            self._forest.inode2deps[inode].add(self)
        self.mark_dirty()

    def load_interned_data(self, d):
        (self.name, self._block_id, self._data) = cbor.loads(d)
        return self

    def to_interned_data(self):
        return cbor.dumps((self.name, self._block_id, self._data))


class DirectoryTreeNode(LoadedTreeNode):
    leaf_class = DirectoryEntry
    entry_type = const.TYPE_DIRNODE


class Forest:

    directory_node_class = DirectoryTreeNode

    def __init__(self, storage, root_inode):
        self.inode2node = {}  # inode -> DirectoryTreeNode root
        self.node2inode = {}  # DirectoryTreeNode root -> inode

        # inode -> DirectoryEntry that has it (via .set_inode)
        self.inode2deps = collections.defaultdict(set)

        self.first_free_inode = root_inode + 1
        self.storage = storage
        self.root_inode = root_inode
        self.dirty_node_set = set()

    def flush(self):
        _debug('flush')
        while self.dirty_node_set:
            self.dirty_node_set, dns = set(), self.dirty_node_set
            for node in dns:
                inode = self.node2inode.get(node)
                if inode:
                    for node2 in self.inode2deps.get(inode, []):
                        node2.mark_dirty()

        r = self.root.flush()
        if r:
            _debug(' new content_id %s', self.root._block_id)
            self.storage.set_block_name(self.root._block_id, b'content')
        return r

    def add_child(self, tn, cn):
        assert tn.parent is None
        ntn = tn.add(cn)
        if not ntn or ntn == tn:
            return
        _debug('root changed for %s to %s', tn, ntn)
        inode = self.node2inode.get(tn)
        if inode:
            del self.node2inode[tn]
            self.node2inode[ntn] = inode
            self.inode2node[inode] = ntn

    def create_dir_inode(self, inode=None):
        if inode is None:
            inode = self.first_free_inode
            self.first_free_inode += 1
        tn = self.directory_node_class(self)
        tn._loaded = True
        tn.dirty = True
        self.inode2node[inode] = tn
        self.node2inode[tn] = inode
        return inode, tn

    def get_dir_inode(self, i):
        if i in self.inode2node:
            return self.inode2node[i]
        if i == self.root_inode:
            block_id = self.storage.get_block_id_by_name(b'content')
            tn = self.load_directory_node_from_block(block_id)
            self.inode2node[i] = tn
            self.node2inode[tn] = i
            return tn

    def load_directory_node_from_block(self, block_id):
        tn = self.directory_node_class(self).load(block_id)
        assert tn.is_loaded
        return tn

    @property
    def root(self):
        return self.get_dir_inode(self.root_inode)
