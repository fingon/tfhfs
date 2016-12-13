#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: forest_nodes.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Dec  3 17:45:55 2016 mstenber
# Last modified: Tue Dec 13 21:08:19 2016 mstenber
# Edit time:     6 min
#
"""

These are the btree node subclasses employed by the Forest class.

"""

import btree
import const
from util import CBORPickler, DataMixin, DirtyMixin, sha256


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
        return self.set_block(block_id, data)

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

    def set_block(self, block_id, data):
        if block_id == self._block_id:
            return
        self._forest.storage.refer_or_store_block(block_id, data)
        if self._block_id is not None:
            self._forest.storage.release_block(self._block_id)
        self._block_id = block_id
        return True

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

    def __init__(self, forest, *, block_id=None, **kw):
        self._forest = forest
        btree.LeafNode.__init__(self, **kw)
        if block_id:
            self._block_id = block_id

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

    @property
    def is_dir(self):
        return (self.data['mode'] & const.DENTRY_MODE_DIR)

    @property
    def is_file(self):
        return not self.is_dir


class DirectoryTreeNode(LoadedTreeNode):
    leaf_class = DirectoryEntry
    entry_type = const.TYPE_DIRNODE


class FileBlockEntry(NamedLeafNode):
    name_hash_size = 0


class FileBlockTreeNode(LoadedTreeNode):
    leaf_class = FileBlockEntry
    entry_type = const.TYPE_FILENODE
