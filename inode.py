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
# Last modified: Sat Nov 26 10:50:00 2016 mstenber
# Edit time:     16 min
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

from btree import LeafNode, TreeNode

_debug = logging.getLogger(__name__).debug


class INodeStore:
    """ INode storage abstraction.

    Essentially part of Forest class, but intentionally abstracted away.
    """

    def __init__(self, first_free_inode):
        self._value2inode = {}
        self._node2inode = {}
        self._pnode2inode = {}
        self.first_free_inode = first_free_inode
        self.inodes_waiting_to_remove = set()

    def add_inode(self, node, *, parent_node=None, value=None):
        assert isinstance(node, TreeNode)
        assert not parent_node or isinstance(parent_node, LeafNode)
        if value is None:
            value = self.first_free_inode
            self.first_free_inode += 1
        inode = INode(self, node, parent_node, value)
        self._register_inode(inode)
        return inode

    def count(self):
        return len(self._node2inode)

    def get_inode_by_node(self, node):
        assert isinstance(node, TreeNode)
        return self._node2inode[node]

    def get_inode_by_parent_node(self, parent_node):
        assert isinstance(parent_node, LeafNode)
        return self._pnode2inode[parent_node]

    def get_inode_by_value(self, value):
        return self._value2inode[value]

    def getdefault_inode_by_node(self, node, default=None):
        return self._node2inode.get(node, default)

    def getdefault_inode_by_value(self, value, default=None):
        return self._value2inode.get(value, default)

    def _register_inode(self, inode):
        self._value2inode[inode.value] = inode
        self._node2inode[inode.node] = inode
        p = inode.parent_node
        if p:
            self._pnode2inode[p] = inode

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
        del self._node2inode[inode.node]
        p = inode.parent_node
        if p:
            del self._pnode2inode[p]


class INode:

    refcnt = 1

    def __init__(self, store, node, parent_node, value):
        self.node = node
        self.parent_node = parent_node
        self.value = value
        self.store = store
        # Add reference to the parent inode; we do not want children dangling
        if parent_node:
            self.store.get_inode_by_node(parent_node.root).ref()

    def deref(self, count=1):
        assert count > 0
        if self.refcnt == count:
            self.store.inodes_waiting_to_remove.add(self)
        self.refcnt -= count
        assert self.refcnt >= 0

    def ref(self, count=1):
        assert count > 0
        assert self.refcnt >= 0
        if self.refcnt == 0:
            self.store.inodes_waiting_to_remove.remove(self)
        self.refcnt += count

    def remove(self):
        assert self.refcnt == 0
        # Derefer parent
        if self.parent_node:
            self.store.get_inode_by_node(self.parent_node.root).deref()
        # Remove from the store
        self.store._unregister_inode(self)

    def set_node(self, node):
        assert isinstance(node, TreeNode)
        if self.node is node:
            return
        _debug('replacing %s node %s -> %s' % (self, self.node, node))
        del self.store._node2inode[self.node]
        assert node not in self.store._node2inode
        self.node = node
        self.store._node2inode[self.node] = self
