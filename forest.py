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
# Last modified: Sat Dec  3 17:47:09 2016 mstenber
# Edit time:     528 min
#
"""This is the 'forest layer' main module.

It implements nested tree concept, with an interface to the storage
layer.

While on-disk snapshot is always static, more recent, in-memory one is
definitely not. Flushed-to-disk parts of the tree may be eventually
purged as desired.

Inode numbers are dynamic and reference counted; they are never stored
to disk, but instead are used only to identify:

- tree roots (which are objects that potentially dynamically change
underneath - as the datastructures are essentially immutable, the
copy-on-write semantics will create e.g. new btree hierarchies on
changes), and

- their parent (leaf) nodes in parent btree (n/a in case of root)

Inode numbers allow access to particular subdirectory even over
mutations. They have explicit reference counting as the fuse library
may refer to a particular inode and/or filehandles referring to a
particular (file) inode.

"""

import logging

import const
import inode
from forest_nodes import DirectoryTreeNode, FileBlockTreeNode

_debug = logging.getLogger(__name__).debug


CONTENT_NAME = b'content'


class Forest(inode.INodeStore):
    """Forest maintains the (nested set of) trees.

    It also keeps track of the dynamic inode entries; when their
    reference count drops to zero, the objects are purged.

    In general, any object that returns something from the API, will
    increment the reference count by 1. Object's deref() method should
    be called to dereference it.
    """

    directory_node_class = DirectoryTreeNode
    file_node_class = FileBlockTreeNode

    def __init__(self, storage, root_inode):
        self.root_inode = root_inode
        self.storage = storage
        self.init()

    def init(self):
        inode.INodeStore.__init__(self, first_free_inode=self.root_inode + 1)
        self.dirty_node_set = set()
        block_id = self.storage.get_block_id_by_name(CONTENT_NAME)
        tn = self.load_dir_node_from_block(block_id)
        tn.load()
        self.root = self.add_inode(tn, value=self.root_inode)

    def _create(self, mode, dir_inode, name):
        # Create 'content tree' root node for the new child
        is_directory = mode & const.DENTRY_MODE_DIR
        if is_directory:
            cl = self.directory_node_class
            node = cl(self)
            node._loaded = True
        else:
            node = None

        leaf = dir_inode.node.leaf_class(self, name=name)

        # Create leaf node for the tree 'rn'
        rn = dir_inode.node
        assert not rn.parent
        self.get_inode_by_node(rn).set_node(rn.add(leaf))
        inode = self.add_inode(node=node, leaf_node=leaf)
        if node:
            node.mark_dirty()
        leaf.set_data('mode', is_directory)
        return inode

    def create_dir(self, dir_inode, name):
        return self._create(const.DENTRY_MODE_DIR, dir_inode, name)

    def create_file(self, dir_inode, name):
        return self._create(0, dir_inode, name)

    def flush(self):
        _debug('flush')
        # Three stages:
        # - first we propagate dirty nodes towards the root
        while self.dirty_node_set:
            self.dirty_node_set, dns = set(), self.dirty_node_set
            for node in dns:
                inode = self.get_inode_by_node(node.root)
                if inode.leaf_node:
                    inode.leaf_node.mark_dirty()

        # Then we call the root node's flush method, which
        # - propagates the calls back down the tree, updates block ids, and
        # - gets back up the tree with fresh block ids.
        rv = self.root.node.flush()
        if rv:
            _debug(' new content_id %s', self.root.node._block_id)
            self.storage.set_block_name(self.root.node._block_id, CONTENT_NAME)

        # Now that the tree is no longer dirty, we can kill inodes
        # that have no reference (TBD: This could also depend on some
        # caching LRU criteria, have to think about it)
        self.remove_old_inodes()

        return rv

    def lookup(self, dir_inode, name):
        assert isinstance(dir_inode, inode.INode)
        assert isinstance(name, bytes)
        n = dir_inode.node.search_name(name)
        if n:
            child_inode = self.getdefault_inode_by_leaf_node(n)
            if child_inode is None:
                mode = n.data['mode']
                if mode & const.DENTRY_MODE_DIR:
                    cn = self.load_dir_node_from_block(n._block_id)
                elif mode & const.DENTRY_MODE_MINIFILE:
                    cn = self.file_node_class.leaf_class(forest=self,
                                                         block_id=n._block_id)
                else:
                    cn = self.load_file_node_from_block(n._block_id)
                child_inode = self.add_inode(cn, leaf_node=n)
            else:
                child_inode.ref()
            return child_inode

    def _load_node_from_block(self, is_dir, block_id):
        cl = is_dir and self.directory_node_class or self.file_node_class
        tn = cl(forest=self, block_id=block_id)
        return tn

    def load_dir_node_from_block(self, block_id):
        return self._load_node_from_block(True, block_id)

    def load_file_node_from_block(self, block_id):
        return self._load_node_from_block(False, block_id)
