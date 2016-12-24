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
# Last modified: Sat Dec 24 09:55:20 2016 mstenber
# Edit time:     663 min
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

import collections
import logging
import stat
import time
import weakref

import inode
import llfuse
from forest_file import FileINode
from forest_nodes import (DirectoryTreeNode, FileBlockTreeNode, FileData,
                          any_node_block_data_references_callback)
from util import Allocator

_debug = logging.getLogger(__name__).debug

PRINT_DEBUG_FLUSH = True


class Forest:
    """Forest maintains the (nested set of) trees.

    It also keeps track of the dynamic inode entries; when their
    reference count drops to zero, the objects are purged.

    In general, any object that returns something from the API, will
    increment the reference count by 1. Object's deref() method should
    be called to dereference it.
    """

    directory_node_class = DirectoryTreeNode
    file_node_class = FileBlockTreeNode

    def __init__(self, storage, *,
                 root_inode=llfuse.ROOT_INODE, content_name=b'content'):
        self.root_inode = root_inode
        self.content_name = content_name
        self.storage = storage
        self.storage.add_block_id_has_references_callback(
            self.inode_has_block_id)
        self.storage.set_block_data_references_callback(
            any_node_block_data_references_callback)
        self.init()

    def init(self):
        self.block_id_references = collections.defaultdict(weakref.WeakSet)
        self.fds = Allocator()
        self.inodes = inode.INodeAllocator(self, self.root_inode)
        self.dirty_node_set = set()
        block_id = self.storage.get_block_id_by_name(self.content_name)
        tn = self.directory_node_class(forest=self, block_id=block_id)
        self.root = self.inodes.add_inode(tn, value=self.root_inode)

    def _create(self, mode, dir_inode, name):
        # Create 'content tree' root node for the new child
        is_directory = stat.S_ISDIR(mode)
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
        self.inodes.get_by_node(rn).add_node_to_tree(leaf)
        inode = self.inodes.add_inode(node=node, leaf_node=leaf,
                                      cl=((not is_directory) and FileINode))
        if node:
            node.mark_dirty()
        leaf.set_data('st_mode', mode)
        t = int(time.time() * 1e9)
        leaf.set_data('st_ctime_ns', t)
        leaf.set_data('st_mtime_ns', t)
        return inode

    def create_dir(self, dir_inode, name, *, mode=0):
        if not stat.S_IFMT(mode):
            mode |= stat.S_IFDIR
        dir_inode.changed()
        _debug('create_dir %s 0x%x', name, mode)
        return self._create(mode, dir_inode, name)

    def create_file(self, dir_inode, name, *, mode=0):
        if not stat.S_IFMT(mode):
            mode |= stat.S_IFREG
        dir_inode.changed()
        _debug('create_file %s 0x%x', name, mode)
        return self._create(mode, dir_inode, name)

    def flush(self):
        if PRINT_DEBUG_FLUSH:
            print('flush')
            import time
            t = time.time()
        # Three stages:
        # - first we propagate dirty nodes towards the root
        while self.dirty_node_set:
            self.dirty_node_set, dns = set(), self.dirty_node_set
            for node in dns:
                inode = self.inodes.get_by_node(node.root)
                if inode.leaf_node:
                    inode.leaf_node.mark_dirty()

        if PRINT_DEBUG_FLUSH:
            print(' .. dirtied', time.time() - t)

        # Then we call the root node's flush method, which
        # - propagates the calls back down the tree, updates block ids, and
        # - gets back up the tree with fresh block ids.
        rv = self.root.node.flush()
        if rv:
            _debug(' new content_id %s', self.root.node.block_id)
            self.storage.set_block_name(self.root.node.block_id,
                                        self.content_name)
        if PRINT_DEBUG_FLUSH:
            print(' .. tree flushed', time.time() - t)

        # TBD: Is there some case where we would not want this?
        self.storage.flush()
        if PRINT_DEBUG_FLUSH:
            print(' .. storage flushed', time.time() - t)

        # Now that the tree is no longer dirty, we can kill inodes
        # that have no reference (TBD: This could also depend on some
        # caching LRU criteria, have to think about it)
        self.inodes.remove_old_inodes()

        # Similarly, we can do unloading of nodes that are not needed
        # (the storage should cache what we need anyway, and it is
        # much more efficient in terms of memory usage than raw Python
        # data structures)
        self.unload_nonprotected_nodes()
        if PRINT_DEBUG_FLUSH:
            print('took', time.time() - t)
        return rv

    def inode_has_block_id(self, block_id):
        for bref in self.block_id_references.get(block_id, []):
            if self.inodes.getdefault_by_node(bref.node):
                return True

    def lookup(self, dir_inode, name):
        assert isinstance(dir_inode, inode.INode)
        assert isinstance(name, bytes)
        n = dir_inode.node.search_name(name)
        if n:
            child_inode = self.inodes.getdefault_by_leaf_node(n)
            if child_inode is None:
                if n.is_dir:
                    cn = self.directory_node_class(forest=self,
                                                   block_id=n.block_id)
                    cl = None
                else:
                    cn = None
                    cl = FileINode
                child_inode = self.inodes.add_inode(cn, leaf_node=n, cl=cl)
            else:
                child_inode.ref()
            return child_inode

    def merge_remote(self, new_other_name, old_other_name):
        new_id = self.storage.get_block_id_by_name(new_other_name)
        assert new_id
        new_f = Forest(self.storage, root_inode=self.root_inode,
                       content_name=new_other_name)
        old_f = None
        old_id = self.storage.get_block_id_by_name(old_other_name)
        if new_id == old_id:
            return
        if old_id:
            old_f = Forest(self.storage, root_inode=self.root_inode,
                           content_name=old_other_name)
        delta = self.merge3(new_f, old_f)
        self.storage.set_block_name(new_id, old_other_name)
        return delta

    def merge3(self, new_other, old_other=None):
        assert not old_other or isinstance(old_other, Forest)
        assert isinstance(new_other, Forest)
        # Essentially, we are looking to get 'stuff' from the other,
        # while updating ourselves IF CHANGE WOULD NOT CONFLICT.

        # These all MUST be in same storage, as we blatantly reuse
        # objects back and forth. 'self' may be in use, new_other and
        # old_other are used read-only and should not cause any
        # mutations to disk.
        return self.merge3_dir_inode(self.root, new_other.root,
                                     old_other and old_other.root)

    def merge3_dir_inode(self, inode, new_inode, old_inode):
        _debug('merge3_dir_inode')
        # TBD: Do things with metadata
        # l = leaf from our tree
        # l2 = leaf from 'new' tree
        # l3 = leaf from 'old' tree if any
        delta = 0
        seen = set()

        def _handle(l, nl):
            if l is not None:
                self.unlink(inode, l.name)
            if nl is None:
                _debug('  removed')
                return
            if l is not None:
                l.set_forest_rec(None)
                _debug('  replaced')
            else:
                _debug('  added')
            new_inode.node.remove_from_tree(nl)
            inode.add_node_to_tree(nl)
            nl.set_forest_rec(self)

        # First step: Look at what we have
        for l in list(inode.node.get_leaves()):
            seen.add(l.key)
            _debug(' %s', l.name)
            l2 = new_inode.node.search_name(l.name)
            if not l2:
                # Other side does not have it.
                l3 = old_inode and old_inode.node.search_name(l.name)
                if l3:
                    # Remote party removed it, we assume it knows what it is
                    # doing.
                    if not l.is_newer_than(l3):
                        _handle(l, None)
                        continue
                # Ok, other end should pick this up? Wait for it..
                delta += 1
                continue

            if l.is_same(l2):
                continue

            if l.is_dir and l2.is_dir:
                # We can just recurse inside
                try:
                    in1 = self.lookup(inode, l.name)
                    in2 = new_inode.store.lookup(new_inode, l.name)
                    in3 = old_inode and old_inode.store.lookup(
                        old_inode, l.name)
                    delta += self.merge3_dir_inode(in1, in2, in3)
                finally:
                    in1.deref()
                    in2.deref()
                    if in3:
                        in3.deref()
                continue

            # Otherwise we grab the newer of the versions
            if l2.is_newer_than(l):
                # Looks like it was replaced? Who knows.  Scary
                # stuff starts here.
                _handle(l, l2)
            else:
                # We are newer?
                delta += 1

        for l2 in list(new_inode.node.get_leaves()):
            if l2.key in seen:
                continue
            _debug(' %s', l2.name)
            l3 = old_inode and old_inode.node.search_name(l2.name)
            if l3:
                # We saw it before but we did not want it; we still do not
                delta += 1
                continue
            # Fine, we want this leaf! 'acquire' (no need to recurse)
            _handle(None, l2)
        if not delta:
            l = inode.direntry
            l2 = new_inode.direntry
            _debug(' unchanged directory; would like to to merge')
            if not l.is_same(l2):
                # Let's try to make it same
                if l2.is_newer_than(l):
                    node = new_inode.node
                    new_inode.set_node(None)
                    node.set_forest_rec(inode.store)
                    inode.set_node(node)
                    _debug('  replaced from remote')
                else:
                    delta += 1
                    _debug('  remote should replace')
        return delta

    def refer_or_store_block_by_data(self, d):
        n = FileData(self, None, d)
        n.perform_flush(in_inode=False)
        return n.block_id

    def unload_nonprotected_nodes(self):
        protected_set = self.inodes.get_protected_set()
        self.root.node.unload_if_possible(protected_set)

    def unlink(self, dir_inode, name):
        n = dir_inode.node.search_name(name)
        assert n
        dir_inode.node.remove_child(n)
        # TBD: Fix also inodes that refer to this? or not?
