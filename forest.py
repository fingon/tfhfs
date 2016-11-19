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
# Last modified: Sat Nov 19 11:39:36 2016 mstenber
# Edit time:     345 min
#
"""This is the 'forest layer' main module.

It implements nested tree concept, with an interface to the storage
layer.

While on-disk snapshot is always static, more recent, in-memory one is
definitely not. Flushed-to-disk parts of the tree may be eventually
purged as desired.

Inode numbers are dynamic and reference counted; they are never stored
to disk, but instead are used only to identify objects that
potentially dynamically change underneath - as the datastructures are
essentially immutable, the copy-on-write semantics will create
e.g. new btree hierarchies on changes, but inode numbers allow access
to particular subdirectory even over mutations.

"""

import collections
import hashlib
import logging

import cbor

import btree
import const
from ms.lazy import lazy_property

_debug = logging.getLogger(__name__).debug


class DirtyMixin:
    dirty = False

    def mark_dirty(self):
        if self.dirty:
            return
        _debug('marked dirty: %s', self)
        self.dirty = True
        self.mark_dirty_related()
        return True

    def mark_dirty_related(self):
        raise NotImplementedError

    def flush(self):
        if not self.dirty:
            return
        del self.dirty
        return self.perform_flush()

    def perform_flush(self):
        raise NotImplementedError


def _sha256(*l):
    h = hashlib.sha256()
    for s in l:
        if isinstance(s, int):
            s = bytes([s])
        h.update(s)
    return h.digest()


class DataMixin(DirtyMixin):
    _data = None

    @property
    def data(self):
        if self._data is None:
            self._data = {}
        return self._data

    def mark_dirty_related(self):
        if self.parent:
            self.parent.mark_dirty()

    def set_data(self, k, v):
        if self._data == None:
            self._data = {}
        if self._data.get(k) is not v:
            self._data[k] = v
            self.mark_dirty()


class CBORPickler:

    def __init__(self, internal2external_dict):
        self.internal2external_dict = internal2external_dict

    @lazy_property
    def external2internal_dict(self):
        d = {v: k for k, v in self.internal2external_dict.items()}
        return d

    def dumps(self, o):
        return cbor.dumps(self.get_dict(o))

    def load_to(self, d, o):
        self.set_dict_to(cbor.loads(d), o)
        return o

    def get_dict(self, o):
        d = {self.internal2external_dict[k]: getattr(o, k)
             for k in self.internal2external_dict.keys()}
        return d

    def set_dict_to(self, d, o):
        for k, v in d.items():
            k2 = self.external2internal_dict[k]
            setattr(o, k2, v)


class LoadedTreeNode(DataMixin, btree.TreeNode):

    # 'pickler' is used to en/decode references to this object within
    # other nodes closer to the root of the tree.
    pickler = CBORPickler(dict(key='k', _block_id='b', _data='d'))

    # 'content_pickler' is used to en/decode content of this object.
    # type is not within, as it is part of 'storage' (as it has also
    # compressed bit). pickled_child_list contains references handled
    # via 'pickler'.
    content_pickler = CBORPickler(dict(key='k',
                                       _data='d',
                                       pickled_child_list='l'))

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
        self.content_pickler.load_to(d, self)
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
        block_id = _sha256(*data)
        if block_id == self._block_id:
            return
        self._forest.storage.refer_or_store_block(block_id, data)
        if self._block_id is not None:
            self._forest.storage.release_block(self._block_id)
        self._block_id = block_id
        return True

    @property
    def pickled_child_list(self):
        return [x.pickler.get_dict(x) for x in self.children]

    @pickled_child_list.setter
    def pickled_child_list(self, child_data_list):
        for cd in child_data_list:
            if self._was_stored_leafy:
                cls2 = self.leaf_class
            else:
                cls2 = self.__class__
            tn2 = cls2(self._forest)
            tn2.pickler.set_dict_to(cd, tn2)
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
    pickler = CBORPickler(dict(name='n', _block_id='b', _data='d'))

    _block_id = None
    _inode = None  # of the child that we represent

    def __init__(self, forest, **kw):
        self._forest = forest
        btree.LeafNode.__init__(self, **kw)

    def perform_flush(self):
        if self._inode is not None:
            c = self._forest.get_inode(self._inode)
            if c:
                c.flush()
                assert c._block_id
                self.set_block_id(c._block_id)
        return True

    def set_block_id(self, block_id):
        if self._block_id == block_id:
            return
        if self._block_id:
            self._forest.storage.release_block(self._block_id)
        self._block_id = block_id
        self._forest.storage.refer_block(block_id)

    def set_inode(self, inode):
        """For the run-time, mark that this object has a whole subtree which
is determined by particular inode."""
        if self._inode == inode:
            return
        if self._inode and self._inode in self._forest.inode2deps:
            self._forest.inode2deps[self._inode].discard(self)
        self._inode = inode
        if inode:
            self._forest.inode2deps[inode].add(self)
        self.mark_dirty()


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


class Forest:

    directory_node_class = DirectoryTreeNode
    file_node_class = FileBlockTreeNode

    def __init__(self, storage, root_inode):
        self.inode2node = {}  # inode -> DirectoryTreeNode root
        self.node2inode = {}  # DirectoryTreeNode root -> inode

        # inode -> DirectoryEntry that has it (via .set_inode)
        self.inode2deps = collections.defaultdict(set)

        self.first_free_inode = root_inode + 1
        self.storage = storage
        self.root_inode = root_inode
        self.dirty_node_set = set()

    def add_child(self, tn, cn):
        assert tn.parent is None
        ntn = tn.add(cn)
        if not ntn or ntn == tn:
            return
        _debug('root changed for %s to %s', tn, ntn)
        inode = self.node2inode.get(tn)
        if inode:
            del self.node2inode[tn]
        self._add_inode(inode, ntn)
        return ntn

    def _add_inode(self, inode, tn):
        self.node2inode[tn] = inode
        self.inode2node[inode] = tn

    def _create(self, is_directory, tn, name):
        inode_sub, sub = self._create_inode(is_directory)
        leaf = sub.leaf_class(self, name=name)
        leaf.set_inode(inode_sub)
        tn = self.add_child(tn, leaf)
        return tn, leaf, sub

    def _create_inode(self, is_directory, *, inode=None, **kw):
        if inode is None:
            inode = self.first_free_inode
            self.first_free_inode += 1
        cl = is_directory and self.directory_node_class or self.file_node_class
        tn = cl(self, **kw)
        tn._loaded = True
        tn.dirty = True
        self._add_inode(inode, tn)
        return inode, tn

    def create_dir(self, tn, name):
        return self._create(True, tn, name)

    def create_file(self, tn, name):
        return self._create(False, tn, name)

    def flush(self):
        _debug('flush')
        while self.dirty_node_set:
            self.dirty_node_set, dns = set(), self.dirty_node_set
            for node in dns:
                inode = self.node2inode.get(node)
                for node2 in self.inode2deps.get(inode, []):
                    node2.mark_dirty()

        r = self.root.flush()
        if r:
            _debug(' new content_id %s', self.root._block_id)
            self.storage.set_block_name(self.root._block_id, b'content')
        return r

    def get_inode(self, i):
        if i in self.inode2node:
            return self.inode2node[i]
        if i == self.root_inode:
            block_id = self.storage.get_block_id_by_name(b'content')
            tn = self.load_dir_node_from_block(block_id)
            self.inode2node[i] = tn
            self.node2inode[tn] = i
            return tn

    def get_node_inode(self, n):
        return self.node2inode.get(n)

    def _load_node_from_block(self, is_dir, block_id):
        cl = is_dir and self.directory_node_class or self.file_node_class
        tn = cl(self).load(block_id)
        assert tn.is_loaded
        return tn

    def load_dir_node_from_block(self, block_id):
        return self._load_node_from_block(True, block_id)

    def load_file_node_from_block(self, block_id):
        return self._load_node_from_block(False, block_id)

    @property
    def root(self):
        return self.get_inode(self.root_inode)
