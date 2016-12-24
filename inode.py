#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: inode.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Fri Nov 25 15:42:50 2016 mstenber
# Last modified: Sat Dec 24 06:55:29 2016 mstenber
# Edit time:     68 min
#
"""

Logical INode store + INode class.

INodes are assumed to be stored in a tree which looks like this:

<ROOT-INODE> <root tree root node>
                      |
                      |
              <leafy parent node> < INODE > <child tree root node>
                  (e.g.dentry)                       |
                                                     |
                                            <leaf parent node> < INODE > ...

"""

import logging
import time

import const
from btree import LeafNode, TreeNode
from forest_nodes import DirectoryEntry
from util import Allocator

_debug = logging.getLogger(__name__).debug


class INodeAllocator(Allocator):
    """ INode storage abstraction.
    """

    def __init__(self, forest, first_free_inode):
        Allocator.__init__(self, first_free_inode)
        self.forest = forest
        self.node2inode = {}
        self.lnode2inode = {}
        self.inodes_waiting_to_remove = set()

    def add_inode(self, node, *, leaf_node=None, cl=None, value=None):
        assert leaf_node is None or isinstance(leaf_node, LeafNode)
        inode = (cl or INode)(self, node, leaf_node)
        if value:
            assert inode.value == value
        return inode

    def get_by_node(self, node):
        assert isinstance(node, TreeNode)
        return self.node2inode[node]

    def get_by_leaf_node(self, leaf_node):
        assert isinstance(leaf_node, LeafNode)
        return self.lnode2inode[leaf_node]

    def get_protected_set(self):
        # Every active inode and their path to root is sacrosanct and
        # should not be unloaded (in theory, we could unload non-dirty
        # ones but it does not seem worth it)
        protected_set = set()
        for inode in self.value2object.values():
            ln = inode.leaf_node
            while ln is not None:
                protected_set.add(ln)
                ln = ln.parent
        _debug('get_protected_set => %d nodes for %d inodes',
               len(protected_set), len(self.node2inode))
        return protected_set

    def getdefault_by_node(self, node, default=None):
        return self.node2inode.get(node, default)

    def getdefault_by_leaf_node(self, leaf_node, default=None):
        return self.lnode2inode.get(leaf_node, default)

    def _inode_leaf_node_changed(self, inode, old_ln, ln):
        if old_ln:
            del self.lnode2inode[old_ln]
        if ln:
            assert ln not in self.lnode2inode
            self.lnode2inode[ln] = inode

    def _inode_node_changed(self, inode, old_n, n):
        if old_n:
            del self.node2inode[old_n]
        if n:
            assert n not in self.node2inode
            self.node2inode[n] = inode

    def register(self, inode):
        Allocator.register(self, inode)
        self._inode_node_changed(inode, None, inode.node)
        self._inode_leaf_node_changed(inode, None, inode.leaf_node)

    def remove_old_inodes(self):
        cnt = 0
        _debug('remove_old_inodes - total:%d, pending:%d',
               self.count(), len(self.inodes_waiting_to_remove))
        while self.inodes_waiting_to_remove:
            self.inodes_waiting_to_remove, tmp = \
                set(), self.inodes_waiting_to_remove
            for node in tmp:
                node.remove()
                cnt += 1
        return cnt

    def unregister(self, inode):
        Allocator.unregister(self, inode)
        self._inode_node_changed(inode, inode.node, None)
        self._inode_leaf_node_changed(inode, inode.leaf_node, None)


class INode:

    refcnt = 1

    def __init__(self, store, node, leaf_node):
        self.node = node
        self.leaf_node = leaf_node
        self.store = store
        # Add reference to the parent inode; we do not want children dangling
        if leaf_node:
            self.store.get_by_node(leaf_node.root).ref()
        self.store.register(self)
        _debug('%s added', self)

    def __repr__(self):
        return '<INode #%d - n:%s ln:%s>' % (self.value, self.node, self.leaf_node)

    def add_node_to_tree(self, n):
        self.set_node(self.node.add_to_tree(n))

    def changed(self):
        if self.leaf_node or (self.node and isinstance(self.node, TreeNode)):
            self.direntry.set_data('st_mtime_ns', int(time.time() * 1e9))

    def deref(self, count=1):
        assert count > 0
        if self.refcnt == count:
            self.store.inodes_waiting_to_remove.add(self)
        self.refcnt -= count
        assert self.refcnt >= 0
        return self

    def flush(self):
        # TBD: Implement more granual flush; this flushes whole fs!
        self.forest.flush()

    @property
    def forest(self):
        return self.store.forest

    @property
    def direntry(self):
        if self.leaf_node is not None:
            return self.leaf_node
        assert self.node is not None
        n = self.node.search_name(b'')
        if n is None:
            n = DirectoryEntry(self.node.forest, name=b'')
            n.set_data('st_mode', const.FS_ROOT_MODE)
            n.set_data('st_uid', const.FS_ROOT_UID)
            self.add_node_to_tree(n)
        return n

    def ref(self, count=1):
        assert count > 0
        assert self.refcnt >= 0
        if self.refcnt == 0:
            self.store.inodes_waiting_to_remove.remove(self)
        self.refcnt += count
        return self

    def remove(self):
        assert self.refcnt == 0
        # Derefer parent
        if self.leaf_node:
            self.store.get_by_node(self.leaf_node.root).deref()
        # Remove from the store
        self.store.unregister(self)

    def set_leaf_node(self, node):
        if self.leaf_node is node:
            return
        _debug('%s leaf_node = %s' % (self, node))
        if self.leaf_node:
            self.leaf_node.mark_dirty()
            self.leaf_node.set_block_id(None)
        self.store._inode_leaf_node_changed(self, self.leaf_node, node)
        self.leaf_node = node
        if node:
            self.leaf_node.mark_dirty()

    def set_node(self, node):
        if self.node is node:
            return
        _debug('%s node = %s' % (self, node))
        self.store._inode_node_changed(self, self.node, node)
        self.node = node
        if self.leaf_node:
            # The node we are associated is dirty .. by association.
            self.leaf_node.mark_dirty()

    @property
    def value(self):
        return self.store.get_value_by_object(self)
