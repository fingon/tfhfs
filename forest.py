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
# Last modified: Sat Jul  2 21:21:57 2016 mstenber
# Edit time:     62 min
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
    def from_forest_block_child_data(cls, forest, d):
        pass


class DirectoryTreeNode(btree.TreeNode):

    def __init__(self, forest):
        self.forest = forest
        btree.TreeNode.__init__(self)

    @classmethod
    def from_forest_block_data(cls, forest, d):
        tn = cls(forest)
        (t, d) = d
        assert t & const.TYPE_MASK == const.TYPE_DIRNODE
        (tn.child_keys, child_data_list) = cbor.loads(d)
        for cd in child_data_list:
            if t & const.BIT_LEAFY:
                tn2 = DirectoryEntry.from_forest_block_child_data(forest, cd)
            else:
                tn2 = cls.from_forest_block_child_data(forest, cd)
            tn._add_child(tn2)

        return tn

    @classmethod
    def from_forest_block_child_data(cls, forest, d):
        pass

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
        data = self.storage.get_block_data_by_id(block_id)
        tn = DirectoryTreeNode()
        # empty, new tree if it did not exist
        if data is None:
            return tn
        # block _did_ exist.
        (t, d) = data
        return DirectoryTreeNode.from_forest_data(self, d)
