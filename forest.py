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
# Last modified: Sat Dec  3 17:25:04 2016 mstenber
# Edit time:     514 min
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

import btree
import const
import inode
from util import CBORPickler, DataMixin, DirtyMixin, sha256

_debug = logging.getLogger(__name__).debug


class LoadedTreeNode(DirtyMixin, btree.TreeNode):

    # 'pickler' is used to en/decode references to this object within
    # other nodes closer to the root of the tree.
    pickler = CBORPickler(dict(key=1, _block_id=2))

    # 'content_pickler' is used to en/decode content of this object.
    # type is not within, as it is part of 'storage' (as it has also
    # compressed bit). pickled_child_list contains references handled
    # via 'pickler'.
    content_pickler = CBORPickler(
        dict(key=0x11, pickled_child_list=0x12))

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

    def create(self):
        return self.__class__(self._forest)

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
        assert t & const.TYPE_MASK == self.entry_type
        self._was_stored_leafy = t & const.BIT_LEAFY
        self.content_pickler.load_external_dict_to(d, self)
        assert len(self._child_keys) == len(self._children)
        return self

    def mark_dirty_related(self):
        super(LoadedTreeNode, self).mark_dirty_related()
        if self.parent is None:
            self._forest.dirty_node_set.add(self)

    def perform_flush(self):
        if not self.is_loaded:
            return
        self.dirty = False
        for child in self.children:
            child.flush()
        data = self.to_data()
        block_id = sha256(*data)
        if block_id == self._block_id:
            return
        self._forest.storage.refer_or_store_block(block_id, data)
        if self._block_id is not None:
            self._forest.storage.release_block(self._block_id)
        self._block_id = block_id
        return True

    @property
    def pickled_child_list(self):
        return [x.pickler.get_external_dict(x) for x in self.children]

    @pickled_child_list.setter
    def pickled_child_list(self, child_data_list):
        for cd in child_data_list:
            if self._was_stored_leafy:
                cls2 = self.leaf_class
            else:
                cls2 = self.__class__
            tn2 = cls2(self._forest)
            tn2.pickler.set_external_dict_to(cd, tn2)
            self._add_child(tn2, skip_dirty=True)

    def search_name(self, name):
        n = self.leaf_class(self._forest, name=name)
        return self.search(n)

    def to_data(self):
        t = self.entry_type
        if self.is_leafy:
            t = t | const.BIT_LEAFY
        return (t, self.content_pickler.dumps(self))


class NamedLeafNode(DataMixin, btree.LeafNode):
    pickler = CBORPickler(dict(name=0x21,
                               _block_id=0x22,  # tree / data node
                               block_data=0x23,  # raw data without subtree
                               _cbor_data=0x24))

    # Used to pickle _data
    cbor_data_pickler = CBORPickler(dict(mode=0x31, foo=0x42))
    # 'foo' is used in tests only, as random metadata

    _block_id = None
    block_data = None

    def __init__(self, forest, **kw):
        self._forest = forest
        btree.LeafNode.__init__(self, **kw)

    def perform_flush(self):
        inode = self._forest.get_inode_by_leaf_node(self)
        if inode:
            c = inode.node
            if c:
                c.flush()
                assert c._block_id
                self.set_block_id(c._block_id)
            else:
                self.set_block_id(None)
        return True

    def set_block_id(self, block_id):
        if self._block_id == block_id:
            return
        if block_id:
            self._forest.storage.refer_block(block_id)
        if self._block_id:
            self._forest.storage.release_block(self._block_id)
        self._block_id = block_id


class DirectoryEntry(NamedLeafNode):
    pass


class DirectoryTreeNode(LoadedTreeNode):
    leaf_class = DirectoryEntry
    entry_type = const.TYPE_DIRNODE


class FileBlockEntry(NamedLeafNode):
    name_hash_size = 0


class FileBlockTreeNode(LoadedTreeNode):
    leaf_class = FileBlockEntry
    entry_type = const.TYPE_FILENODE


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

        # New = dirty
        leaf.set_data('mode', is_directory)
        if node:
            node.mark_dirty()
        else:
            leaf.mark_dirty()
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
                if n.data['mode'] & const.DENTRY_MODE_DIR:
                    cn = self.load_dir_node_from_block(n._block_id)
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
