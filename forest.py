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
# Last modified: Thu Jun 30 16:54:28 2016 mstenber
# Edit time:     27 min
#
"""This is the 'forest layer' main module.

It implements nested tree concept, with an interface to the storage
layer.

While on-disk snapshot is always static, more recent, in-memory one is
definitely not. Flushed-to-disk parts of the tree m ay be eventually
purged as desired.

"""

import hashlib

import btree
from endecode import Decoder, Encoder

TYPE_DIR_NODE = 1
TYPE_FILE_TREE_NODE = 2
TYPE_FILE_DATA = 3


class DirectoryEntry(btree.LeafNode):
    pass


class DirectoryTreeNode(btree.TreeNode):

    @classmethod
    def from_data(cls, d):
        tn = cls()


def _sha256(*l):
    h = hashlib.sha256()
    for s in l:
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
        assert t == TYPE_DIR_NODE
        return DirectoryTreeNode.from_data(data)
