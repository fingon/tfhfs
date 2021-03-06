#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: forest.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Jun 25 15:36:58 2016 mstenber
# Last modified: Fri Dec 30 16:45:32 2016 mstenber
# Edit time:     323 min
#
"""This is the 'btree' module.

It implements abstract COW-friendly B+ trees.

"""

import bisect
import logging

import const
import mmh3
from ms.lazy import lazy_property

_debug = logging.getLogger(__name__).debug

HASH_SIZE = 32  # bytes
NAME_HASH_SIZE = 4  # bytes
# NAME_HASH_SIZE = 0  # none (for debugging use only)
NAME_SIZE = 256  # maximum length of single name
DEBUG = False  # Special flag which absolutely turns off logging here


class Node:

    header_size = NAME_HASH_SIZE + HASH_SIZE

    parent = None

    @property
    def root(self):
        if self.parent is None:
            return self
        return self.parent.root


class LeafNode(Node):

    name_hash_size = NAME_HASH_SIZE

    def __init__(self, name=None):
        if name is not None:
            assert isinstance(name, bytes)
            self.name = name

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)

    @lazy_property
    def key(self):
        b = mmh3.hash_bytes(self.name)[:self.name_hash_size]
        return b + self.name

    @lazy_property
    def size(self):
        return self.header_size + len(self.name)


class TreeNode(Node):
    """ A class which represents a single intermediate or root tree
node. """

    maximum_size = const.BLOCK_SIZE_LIMIT
    minimum_size = maximum_size * 1 / 4
    has_spares_size = maximum_size / 2

    key = None

    # Note: minimum_size + has_spares_size MUST be less than maximum_size

    def __init__(self):
        self._children = []
        self._child_keys = []
        self.csize = 0
        self.key = None

    def __repr__(self):
        keystring = ''
        if self.key is not None:
            keystring = '>= %s ' % self.key
        return '<%s id%s %s- depth %d>' % (self.__class__.__name__,
                                           id(self),
                                           keystring, self.depth)

    def add_child_nocheck(self, c, *, skip_dirty=False, idx=None):
        assert isinstance(c, Node)
        self.csize += c.size
        k = c.key
        assert k
        if idx is None:
            idx = bisect.bisect(self.child_keys, k)
        self.child_keys.insert(idx, k)
        self.children.insert(idx, c)
        c.parent = self
        if not skip_dirty:
            self.mark_dirty()
        return idx

    def _pop_child(self, idx, **kw):
        c = self.children[idx]
        self.remove_child_nocheck(c, idx=idx, **kw)
        return c

    def remove_child_nocheck(self, c, *, idx=None, skip_dirty=False):
        assert isinstance(c, Node)
        self.csize -= c.size
        if idx is None:
            idx = self.child_keys.index(c.key)
        del self.children[idx]
        del self.child_keys[idx]
        if not skip_dirty:
            self.mark_dirty()

    def _update_key_maybe(self, *, force=False):
        nk = self.child_keys[0]
        if not self.parent or (not force and self.key <= nk):
            return
        idx = self.parent.child_keys.index(self.key)
        self.parent.child_keys[idx] = nk
        self.key = nk
        if not idx:
            self.parent._update_key_maybe()

    def add_child(self, c):
        _debug('add_child %s', c)
        idx = self.add_child_nocheck(c)
        if not idx:
            self._update_key_maybe()
        if self.csize <= self.maximum_size:
            return
        _debug(' too big, splitting')
        tn = self.create()
        while self.csize > tn.csize:
            tn.add_child_nocheck(self._pop_child(-1, skip_dirty=True),
                                 idx=0, skip_dirty=True)
        tn.key = tn.child_keys[0]
        if self.parent is not None:
            self.parent.add_child(tn)
            tn.mark_dirty()
            return
        tn2 = self.create()
        while self.children:
            tn2.add_child_nocheck(self._pop_child(-1, skip_dirty=True),
                                  idx=0, skip_dirty=True)
        tn2.key = tn2.child_keys[0]
        self.add_child_nocheck(tn2, idx=0)
        self.add_child_nocheck(tn, idx=1)
        tn.mark_dirty()
        tn2.mark_dirty()

    def add_to_tree(self, c):
        _debug('adding %s to %s', c, self)
        sc = self.search_prev_or_eq(c)
        if sc:
            assert sc.key != c.key
            _debug(' closest match: %s', sc)
            sc.parent.add_child(c)
            return
        self.add_child(c)

    @property
    def children(self):
        return self._children

    @property
    def child_keys(self):
        return self._child_keys

    def create(self):
        """ Given 'self', create another instance of same class. """
        return self.__class__()

    @property
    def depth(self):
        if self.is_leafy:
            return 1
        return self.children[0].depth + 1

    @property
    def first_leaf(self):
        n = self.children[0]
        if isinstance(n, TreeNode):
            return n.first_leaf
        return n

    @property
    def last_leaf(self):
        n = self.children[-1]
        if isinstance(n, TreeNode):
            return n.last_leaf
        return n

    def get_leaves(self):
        for child in self.children:
            if not isinstance(child, TreeNode):
                yield child
            else:
                yield from child.get_leaves()

    def get_smaller_sib(self):
        if not self.parent:
            return
        idx = self.parent.child_keys.index(self.key) - 1
        if idx >= 0:
            return self.parent.children[idx]

    def get_larger_sib(self):
        if not self.parent:
            return
        idx = self.parent.child_keys.index(self.key) + 1
        if idx < len(self.parent.children):
            return self.parent.children[idx]

    @property
    def is_leafy(self):
        return not self.children or not isinstance(self.children[0], TreeNode)

    def mark_dirty(self):
        """Mark the node dirty. If it returns true, the set was 'new' (and
should be propagated upwards in the tree)."""
        return False

    def remove_child(self, c):
        self.remove_child_nocheck(c)
        if self.csize >= self.minimum_size:
            return

        def _equalize(sib, idx):
            if sib.csize >= self.has_spares_size:
                while sib.csize >= self.csize:
                    self.add_child_nocheck(sib._pop_child(idx))
                if idx == -1:
                    self._update_key_maybe(force=True)
                else:
                    sib._update_key_maybe(force=True)
                return True

        sib = self.get_smaller_sib()
        if sib and _equalize(sib, -1):
            return

        sib2 = self.get_larger_sib()
        if sib2 and _equalize(sib2, 0):
            return

        # If there is no siblings, we cannot merge with anyone, we're
        # last one left..
        if not sib and not sib2:
            return
        if sib and sib2:
            if sib.csize > sib2.csize:
                sib = sib
        elif sib2:
            sib = sib2
        assert self.parent

        # Cannot trigger rebalance; siblings are at <=
        # has_spares_size; so remove us instead after adding what is
        # left to the smaller of the siblings
        while self.children:
            sib.add_child_nocheck(self._pop_child(0))

        sib._update_key_maybe()

        self.parent.remove_child(self)

    @lazy_property
    def size(self):
        return self.header_size + NAME_SIZE

    def remove_from_tree(self, c):
        sc = self.search(c)
        assert sc
        sc.parent.remove_child(sc)
        if self.is_leafy:
            return
        ts = 0
        for c in self.children:
            ts += c.csize
            if ts >= self.minimum_size:
                return

        # Ok. We did not bail yet -> it seems we are really out of
        # children and might as well merge all their content to ours.
        for c in self.children[:]:
            self.remove_child_nocheck(c)
            for c2 in c.children:
                self.add_child_nocheck(c2)

        # this is root so  no need to worry about .key handling..

    def search_prev_or_eq(self, c):
        if DEBUG:
            _debug('search_prev_or_eq %s in %s', c, self)
        n = self
        k = c.key
        while True:
            if not n.child_keys:
                if DEBUG:
                    _debug(' no children')
                return
            if DEBUG:
                _debug(' child_keys %s', n.child_keys)
            idx = bisect.bisect_right(n.child_keys, k)
            if idx:
                idx -= 1
            if DEBUG:
                _debug(' idx %d for %s', idx, k)
            n = n.children[idx]
            if DEBUG:
                _debug(' = %s', n)
            if not isinstance(n, TreeNode):
                return n

    def search(self, c):
        sc = self.search_prev_or_eq(c)
        if sc and sc.key == c.key:
            return sc
