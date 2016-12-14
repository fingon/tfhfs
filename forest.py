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
# Last modified: Wed Dec 14 10:06:19 2016 mstenber
# Edit time:     551 min
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
from forest_file import FDStore, FileINode
from forest_nodes import DirectoryTreeNode, FileBlockTreeNode, FileData

_debug = logging.getLogger(__name__).debug


CONTENT_NAME = b'content'


class Forest(inode.INodeStore, FDStore):
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
        self.storage.set_block_id_has_references_callback(
            self.inode_has_block_id)
        self.init()

    def init(self):
        FDStore.__init__(self)
        inode.INodeStore.__init__(self, first_free_inode=self.root_inode + 1)
        self.dirty_node_set = set()
        block_id = self.storage.get_block_id_by_name(CONTENT_NAME)
        tn = self.directory_node_class(forest=self, block_id=block_id)
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
        inode = self.add_inode(node=node, leaf_node=leaf,
                               cl=((not is_directory) and FileINode))
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
            _debug(' new content_id %s', self.root.node.block_id)
            self.storage.set_block_name(self.root.node.block_id, CONTENT_NAME)

        # Now that the tree is no longer dirty, we can kill inodes
        # that have no reference (TBD: This could also depend on some
        # caching LRU criteria, have to think about it)
        self.remove_old_inodes()

        return rv

    def inode_has_block_id(self, block_id):
        # TBD: This is not super efficient. However, it SHOULD be
        # called only when deleting blocks, which should not be that
        # common occurence (assume read-heavy workloads). This could
        # use a lazy property of some kind, perhaps..
        for node in self._node2inode.keys():
            if node.block_id == block_id:
                return True

    def lookup(self, dir_inode, name):
        assert isinstance(dir_inode, inode.INode)
        assert isinstance(name, bytes)
        n = dir_inode.node.search_name(name)
        if n:
            child_inode = self.getdefault_inode_by_leaf_node(n)
            if child_inode is None:
                if n.is_dir:
                    cn = self.directory_node_class(forest=self,
                                                   block_id=n.block_id)
                    cl = None
                else:
                    cn = None
                    cl = FileINode
                child_inode = self.add_inode(cn, leaf_node=n, cl=cl)
            else:
                child_inode.ref()
            return child_inode

    def refer_or_store_block_by_data(self, d):
        n = FileData(self, None, d)
        n.perform_flush(in_inode=False)
        return n.block_id
