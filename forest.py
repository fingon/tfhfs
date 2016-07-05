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
# Last modified: Tue Jul  5 13:18:48 2016 mstenber
# Edit time:     153 min
#
"""This is the 'forest layer' main module.

It implements nested tree concept, with an interface to the storage
layer.

While on-disk snapshot is always static, more recent, in-memory one is
definitely not. Flushed-to-disk parts of the tree m ay be eventually
purged as desired.

"""

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
        return True

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
            return {}
        return self._data

    def set(self, k, v):
        if self._data == None:
            self._data = {}
        if self._data.get(k) is not v:
            self._data[k] = v
            if self.mark_dirty() and self.parent:
                self.parent.mark_dirty()


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
        assert t & const.TYPE_MASK == const.TYPE_DIRNODE
        (self.key, self._data, child_data_list) = cbor.loads(d)
        for cd in child_data_list:
            if t & const.BIT_LEAFY:
                cls2 = self.leaf_class
            else:
                cls2 = cls2
            tn2 = cls2(self._forest).load_interned_data(cd)
            self._add_child(tn2, skip_dirty=True)
        assert len(self._child_keys) == len(self._children)
        return self

    def to_data(self):
        # n/a: 'key' (should be known already)
        l = [self.key, self.data,
             [x.to_interned_data() for x in self.children]]
        t = self.entry_type
        if self.is_leafy:
            t = t | const.BIT_LEAFY
        return (t, cbor.dumps(l))


class DirectoryEntry(DataMixin, btree.LeafNode):

    def __init__(self, forest):
        self._forest = forest
        btree.LeafNode.__init__(self)

    def perform_flush(self):
        # Not interested in keeping track of self.data;
        # our main purpose is just cause parent to get re-encoded.
        return True

    def load_interned_data(self, d):
        (self.name, self._data) = cbor.loads(d)
        return self

    def to_interned_data(self):
        return cbor.dumps((self.name, self._data))


class DirectoryTreeNode(LoadedTreeNode):

    leaf_class = DirectoryEntry
    entry_type = const.TYPE_DIRNODE

    def perform_flush(self):
        if not self.is_loaded:
            return
        self.dirty = False
        for child in self.children:
            child.flush()
        data = self.to_data()
        block_id = _sha256(*data)
        if block_id == self._block_id:
            _debug(' %s block_id same despite flush: %s', self, block_id)
            return
        self._forest.storage.refer_or_store_block(block_id, data)
        if self._block_id is not None:
            self._forest.storage.release_block(self._block_id)
        self._block_id = block_id
        return True

    def load_interned_data(self, d):
        (self.key, self._block_id, self.data) = cbor.loads(d)
        return self

    def to_interned_data(self):
        return cbor.dumps(self.key, self._block_id, self.data)


class Forest:

    def __init__(self, storage, root_inode):
        self.inode2tree = {}  # inode -> root of the NodeTree
        self.first_free_inode = root_inode + 1
        self.storage = storage
        self.root_inode = root_inode

    def flush(self):
        _debug('flush')
        r = self.root.flush()
        if r:
            _debug(' new content_id %s', self.root._block_id)
            self.storage.set_block_name(self.root._block_id, b'content')
        return r

    def get_dir_inode(self, i):
        if i in self.inode2tree:
            return self.inode2tree[i]
        if i == self.root_inode:
            block_id = self.storage.get_block_id_by_name(b'content')
            tn = self.load_directory_node_from_block(block_id)
            self.inode2tree[i] = tn
            return tn

    def load_directory_node_from_block(self, block_id):
        tn = DirectoryTreeNode(self).load(block_id)
        assert tn.is_loaded
        return tn

    @property
    def root(self):
        return self.get_dir_inode(self.root_inode)
