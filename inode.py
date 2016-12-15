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
# Last modified: Fri Dec 16 08:06:44 2016 mstenber
# Edit time:     43 min
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

_debug = logging.getLogger(__name__).debug


class INodeStore:
    """ INode storage abstraction.

    Essentially part of Forest class, but intentionally abstracted away.
    """

    def __init__(self, first_free_inode):
        self._value2inode = {}
        self._node2inode = {}
        self._lnode2inode = {}
        self.first_free_inode = first_free_inode
        self.inodes_waiting_to_remove = set()

    def add_inode(self, node, *, leaf_node=None, value=None, cl=None):
        assert leaf_node is None or isinstance(leaf_node, LeafNode)
        if value is None:
            value = self.first_free_inode
            self.first_free_inode += 1
        inode = (cl or INode)(self, node, leaf_node, value)
        self._register_inode(inode)
        return inode

    def count(self):
        return len(self._node2inode)

    def get_inode_by_node(self, node):
        assert isinstance(node, TreeNode)
        return self._node2inode[node]

    def get_inode_by_leaf_node(self, leaf_node):
        assert isinstance(leaf_node, LeafNode)
        return self._lnode2inode[leaf_node]

    def get_inode_by_value(self, value):
        assert isinstance(value, int)
        return self._value2inode[value]

    def getdefault_inode_by_node(self, node, default=None):
        return self._node2inode.get(node, default)

    def getdefault_inode_by_leaf_node(self, leaf_node, default=None):
        return self._lnode2inode.get(leaf_node, default)

    def getdefault_inode_by_value(self, value, default=None):
        return self._value2inode.get(value, default)

    def _register_inode(self, inode):
        self._value2inode[inode.value] = inode
        n = inode.node
        if n:
            self._node2inode[inode.node] = inode
        p = inode.leaf_node
        if p:
            self._lnode2inode[p] = inode

    def remove_old_inodes(self):
        cnt = 0
        while self.inodes_waiting_to_remove:
            self.inodes_waiting_to_remove, tmp = \
                set(), self.inodes_waiting_to_remove
            for node in tmp:
                node.remove()
                cnt += 1
        return cnt

    def _unregister_inode(self, inode):
        del self._value2inode[inode.value]
        n = inode.node
        if n:
            del self._node2inode[n]
        p = inode.leaf_node
        if p:
            del self._lnode2inode[p]


class INode:

    refcnt = 1

    def __init__(self, store, node, leaf_node, value):
        self.node = node
        self.leaf_node = leaf_node
        self.value = value
        self.store = store
        # Add reference to the parent inode; we do not want children dangling
        if leaf_node:
            self.store.get_inode_by_node(leaf_node.root).ref()
        _debug('%s added', self)

    def __repr__(self):
        return '<INode #%d - n:%s ln:%s>' % (self.value, self.node, self.leaf_node)

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
        self.store.flush()

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
            self.node.add_child(n)
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
            self.store.get_inode_by_node(self.leaf_node.root).deref()
        # Remove from the store
        self.store._unregister_inode(self)

    def set_leaf_node(self, node):
        if self.leaf_node is node:
            return
        _debug('%s leaf_node = %s' % (self, node))
        assert node not in self.store._lnode2inode
        if self.leaf_node:
            self.leaf_node.mark_dirty()
            del self.store._lnode2inode[self.leaf_node]
            self.leaf_node.set_block_id(None)
        self.leaf_node = node
        if node:
            self.store._lnode2inode[node] = self
            # The node we are associated is dirty .. by association.
            self.leaf_node.mark_dirty()

    def set_node(self, node):
        if self.node is node:
            return
        _debug('%s node = %s' % (self, node))
        assert node not in self.store._node2inode
        if self.node:
            del self.store._node2inode[self.node]
        self.node = node
        if self.node:
            self.store._node2inode[self.node] = self
        if self.leaf_node:
            # The node we are associated is dirty .. by association.
            self.leaf_node.mark_dirty()
