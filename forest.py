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
# Last modified: Mon Jul  4 21:20:33 2016 mstenber
# Edit time:     80 min
#
"""This is the 'forest layer' main module.

It implements nested tree concept, with an interface to the storage
layer.

While on-disk snapshot is always static, more recent, in-memory one is
definitely not. Flushed-to-disk parts of the tree m ay be eventually
purged as desired.

"""

import hashlib

import cbor

import btree
import const


class DirectoryEntry(btree.LeafNode):

    @classmethod
    def from_forest_interned_data(cls, forest, d):
        raise NotImplementedError


class DirectoryTreeNode(btree.TreeNode):

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

    @classmethod
    def from_forest_block_id(cls, forest, block_id):
        tn = cls(forest)
        tn._block_id = block_id
        tn.load()
        return tn

    @classmethod
    def from_forest_interned_data(cls, forest, d):
        raise NotImplementedError

    def load(self):
        assert not self._loaded
        data = self._forest.storage.get_block_data_by_id(self._block_id)
        if data is not None:
            self.load_from_forest_data(self._block_id, data)
        else:
            # otherwise we are already empty node
            self._loaded = True

    def load_from_forest_data(self, block_id, d):
        self._loaded = True
        self._block_id = block_id
        (t, d) = d
        assert t & const.TYPE_MASK == const.TYPE_DIRNODE
        (self.key, self.child_keys, child_data_list) = cbor.loads(d)
        for cd in child_data_list:
            if t & const.BIT_LEAFY:
                cls2 = DirectoryEntry
            else:
                cls2 = cls2
            tn2 = cls2.from_forest_interned_data(self._forest, cd)
            self._add_child(tn2)
        return self

    def to_block_data(self):
        # n/a: 'key' (should be known already)
        l = [self.child_keys,
             [x.to_block_data_child() for x in self.children]]
        t = const.TYPE_DIRNODE
        if self.leafy:
            t = t | const.BIT_LEAFY
        return (t, cbor.dumps(l))

    def to_block_data_child(self):
        return self.data


def _sha256(*l):
    h = hashlib.sha256()
    for s in l:
        if isinstance(s, int):
            s = bytes([s])
        h.update(s)
    h.digest()


class Forest:

    def __init__(self, storage, root_inode):
        self.inode2tree = {}  # inode -> root of the NodeTree
        self.inode2dentries = {}  # inode -> (key, subtree-inode); dirty ones
        self.first_free_inode = root_inode + 1
        self.storage = storage
        self.root_inode = root_inode
        block_id = self.storage.get_block_id_by_name(b'content')
        tn = self.load_directory_node_from_block(block_id)
        self.inode2tree[root_inode] = tn

    def load_directory_node_from_block(self, block_id):
        return DirectoryTreeNode.from_forest_block_id(self, block_id)
