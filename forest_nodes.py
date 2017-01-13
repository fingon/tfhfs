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
# Last modified: Fri Jan 13 12:51:44 2017 mstenber
# Edit time:     143 min
#
"""

These are the btree node subclasses employed by the Forest class.

"""

import logging
import stat
import weakref

import btree
import const
from util import CBORPickler, DataMixin, DirtyMixin, sha256

_debug = logging.getLogger(__name__).debug


class BlockIdReference:
    forest = None

    def __init__(self, node, block_id):
        self._node = weakref.ref(node)
        self.forest = node.forest
        self.value = block_id
        self.forest.block_id_references[self.value].add(self)

    def __del__(self):
        if self.forest:
            try:
                self.unregister()
            except KeyError:
                # If our weakref is already gone, it is hopefully all good..
                pass

    @property
    def node(self):
        return self._node()

    def unregister(self):
        assert self.forest is not None
        d = self.forest.block_id_references[self.value]
        d.remove(self)
        if not d:
            del self.forest.block_id_references[self.value]
        del self.forest


class BlockIdReferrerMixin:
    _block_id = None

    @property
    def block_id(self):
        bid = self._block_id
        if bid:
            if isinstance(bid, BlockIdReference):
                return bid.value
            assert isinstance(bid, bytes)
        return bid

    @block_id.setter
    def block_id(self, v):
        if self.block_id == v:
            return
        if isinstance(self._block_id, BlockIdReference):
            self._block_id.unregister()
        if v and self.forest:
            v = BlockIdReference(self, v)
        self._block_id = v

    def set_forest_rec(self, forest):
        assert forest
        # While this looks crazy, it is actually correct, as it sets
        # things using the above setter (removing reference from
        # forest we leave, and adding it to forest we join)
        bid = self.block_id
        if bid:
            self.block_id = None
        self.forest = forest
        if bid:
            self.block_id = bid


class LoadedTreeNode(BlockIdReferrerMixin, DirtyMixin, btree.TreeNode):

    # 'pickler' is used to en/decode references to this object within
    # other nodes closer to the root of the tree.
    pickler = CBORPickler(dict(key=1, block_id=2))

    # 'content_pickler' is used to en/decode content of this object.
    # type is not within, as it is part of 'storage' (as it has also
    # compressed bit). pickled_child_list contains references handled
    # via 'pickler'.
    content_pickler = CBORPickler(
        dict(key=0x11, pickled_child_list=0x12))

    _loaded = False

    def __init__(self, forest, block_id=None):
        self.forest = forest
        self.block_id = block_id
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
        return self.__class__(self.forest)

    def get_block_ids(self):
        for child in self.children:
            yield child.block_id

    @property
    def is_loaded(self):
        return self._loaded

    def load(self, block_id=None):
        assert not self._loaded
        if block_id is not None:
            self.block_id = block_id
        data = self.forest.storage.get_block_data_by_id(self.block_id)
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
        if self.parent is not None:
            return
        self.forest.dirty_node_set.add(self)
        # self.forest.inodes.get_by_node(self)  # debug check
        # ^ above is almost always correct, but if node is moving
        # from forest to another, it is not

    def perform_flush(self):
        if not self.is_loaded:
            return
        self.dirty = False
        for child in self.children:
            child.flush()
        data = self.to_data()
        block_id = sha256(self.forest.storage.block_id_key, *data)
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
            tn2 = cls2(self.forest)
            tn2.pickler.set_external_dict_to(cd, tn2)
            self.add_child_nocheck(tn2, skip_dirty=True)

    def search_name(self, name):
        n = self.leaf_class(self.forest, name=name)
        return self.search(n)

    def set_block(self, block_id, data):
        if block_id == self.block_id:
            return
        self.forest.storage.refer_or_store_block(block_id, data)
        if self.block_id is not None:
            self.forest.storage.release_block(self.block_id)
        self.block_id = block_id
        return True

    def set_forest_rec(self, forest):
        BlockIdReferrerMixin.set_forest_rec(self, forest)
        if not self._loaded:
            return
        self.mark_dirty()
        for child in self._children:
            child.set_forest_rec(forest)

    def to_data(self):
        t = self.entry_type
        if self.is_leafy:
            t = t | const.BIT_LEAFY
        return (t, self.content_pickler.dumps(self))

    def unload_if_possible(self, protected_set):
        if not self._loaded or self.dirty:
            # subtree not applicable
            return
        for child in self.children:
            child.unload_if_possible(protected_set)
        if self in protected_set:
            # if we are protected, _we_ are not applicable
            return
        self.__init__(self.forest, self.block_id)
        del self._loaded
        _debug('unloaded %s', self)


class NamedLeafNode(BlockIdReferrerMixin, DataMixin, btree.LeafNode):
    pickler = CBORPickler(dict(name=0x21,
                               block_id=0x22,  # tree / data node
                               block_data=0x23,  # raw data without subtree
                               _cbor_data=0x24))

    # Used to pickle _data
    cbor_data_pickler_dict = dict(xattr=0x30, minifile=0x31,
                                  # ^ DirEntry
                                  access_count=0x32, normal_block_id=0x33,
                                  # ^ WeakRefEntry
                                  foo=0x42,
                                  # ^ test only
                                  )
    for v, k in enumerate(const.ATTR_STAT_KEYS, 0x50):
        cbor_data_pickler_dict[k] = v
    # ^ DirEntry st_* are 0x50+

    cbor_data_pickler = CBORPickler(cbor_data_pickler_dict)

    block_id = None
    block_data = None

    def __init__(self, forest, *, block_id=None, **kw):
        self.forest = forest
        btree.LeafNode.__init__(self, **kw)
        if block_id:
            self.block_id = block_id

    def perform_flush(self):
        inode = self.forest.inodes.getdefault_by_leaf_node(self)
        if inode:
            c = inode.node
            if c:
                c.flush()
                assert c.block_id
                self.set_block_id(c.block_id)
            else:
                self.set_block_id(None)
        return True

    def set_block_id(self, block_id):
        if self.block_id == block_id:
            return
        if block_id:
            self.forest.storage.refer_block(block_id)
        if self.block_id:
            self.forest.storage.release_block(self.block_id)
        self.block_id = block_id

    def set_forest_rec(self, forest):
        assert forest
        BlockIdReferrerMixin.set_forest_rec(self, forest)
        self.mark_dirty()
        inode = self.forest.inodes.getdefault_by_leaf_node(self)
        if inode:
            if inode.node:
                inode.node.set_forest_rec(forest)

    def unload_if_possible(self, protected_set):
        inode = self.forest.inodes.getdefault_by_leaf_node(self)
        if inode and inode.node:
            inode.node.unload_if_possible(protected_set)


class DirectoryEntry(NamedLeafNode):

    @property
    def is_dir(self):
        return stat.S_ISDIR(self.mode)

    @property
    def is_file(self):
        return stat.S_ISREG(self.mode)

    def is_same(self, o):
        # TBD: Care about something else too?
        if self.block_id != o.block_id:
            return False
        i1 = sorted(self.pickler.get_internal_dict_items(self))
        i2 = sorted(o.pickler.get_internal_dict_items(o))
        return i1 == i2

    def is_newer_than(self, o):
        mtime = self.data.get('st_mtime_ns', 0)
        o_mtime = o.data.get('st_mtime_ns', 0)
        return mtime > o_mtime

    @property
    def is_sticky(self):
        return self.mode & stat.S_ISVTX

    @property
    def mode(self):
        return self.data['st_mode']

    def set_block_data(self, s):
        if self.block_data == s:
            return
        self.block_data = s
        self.mark_dirty()

    def set_clear_mode_bits(self, set_bits, clear_bits):
        nm = (self.mode | set_bits) & ~clear_bits
        if nm == self.mode:
            return
        self.set_data('st_mode', nm)


class DirectoryTreeNode(LoadedTreeNode):
    leaf_class = DirectoryEntry
    entry_type = const.TYPE_DIRNODE


class FileBlockEntry(NamedLeafNode):
    name_hash_size = 0

    @property
    def content(self):
        # TBD: Do we want to cache this or not?
        return FileData(self.parent.forest, self.block_id, None).content


class FileBlockTreeNode(LoadedTreeNode):
    leaf_class = FileBlockEntry
    entry_type = const.TYPE_FILENODE


class FileData(DirtyMixin, BlockIdReferrerMixin):
    entry_type = const.TYPE_FILEDATA
    block_data = None

    def __init__(self, forest, block_id, block_data):
        self.forest = forest
        self.block_id = block_id
        if block_data is not None:
            self.block_data = block_data
            self.mark_dirty()

    @property
    def content(self):
        if self.block_data is None:
            data = self.forest.storage.get_block_data_by_id(self.block_id)
            (t, d) = data
            assert t == self.entry_type
            return d
            # TBD: Do we really WANT to store data locally?
            #self.block_data = d
        return self.block_data

    def perform_flush(self, *, in_inode=True):
        assert self.block_data is not None
        bd = (self.entry_type, self.block_data)
        bid = sha256(self.forest.storage.block_id_key, *bd)
        if self.block_id == bid:
            return
        self.forest.storage.refer_or_store_block(bid, bd)
        self.block_id = bid
        if in_inode:
            self.forest.storage.release_block(bid)
            # we SHOULD be fine, as we are INode.node
            # -> block_id does not disappear even in refcnt 0 immediately.
        del self.block_data
        return True

    def unload_if_possible(self, protected_set):
        pass


class WeakRefEntry(NamedLeafNode):
    name_hash_size = 0


class WeakRefNode(LoadedTreeNode):
    leaf_class = WeakRefEntry
    entry_type = const.TYPE_WEAKREFNODE

_type_to_loaded_tree_node_subclass = {}

for cl in LoadedTreeNode.__subclasses__():
    _type_to_loaded_tree_node_subclass[cl.entry_type] = cl


def any_node_block_data_references_callback(block_data, *, ignore_weak=False):
    (rt, d) = block_data
    if rt & const.BIT_WEAK and not ignore_weak:
        return
    t = rt & const.TYPE_MASK
    cl = _type_to_loaded_tree_node_subclass.get(t)
    if cl is not None:
        n = cl(None)
        n.load_from_data(block_data)
    elif t == const.TYPE_FILEDATA:
        return
    else:
        assert False, 'unsupported type #%d' % t
    assert isinstance(n, LoadedTreeNode)
    yield from n.get_block_ids()
