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
# Last modified: Tue Jul  5 13:19:01 2016 mstenber
# Edit time:     280 min
#
"""This is the 'btree' module.

It implements abstract COW-friendly B+ trees.

"""

import bisect
import logging

import mmh3
from ms.lazy import lazy_property

_debug = logging.getLogger(__name__).debug

HASH_SIZE = 32  # bytes
NAME_HASH_SIZE = 4  # bytes
# NAME_HASH_SIZE = 0  # none (for debugging use only)
NAME_SIZE = 256  # maximum length of single name


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

    maximum_size = 128000
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
        return '<%s >=%s - depth %d>' % (self.__class__.__name__,
                                         self.key, self.depth)

    def _add_child(self, c, *, skip_dirty=False):
        assert isinstance(c, Node)
        self.csize += c.size
        k = c.key
        assert k
        idx = bisect.bisect(self.child_keys, k)
        self.child_keys.insert(idx, k)
        self.children.insert(idx, c)
        c.parent = self
        if not skip_dirty and self.mark_dirty() and self.parent:
            self.parent.mark_dirty()
        return idx

    def _pop_child(self, idx):
        c = self.children[idx]
        self._remove_child(c)
        return c

    def _remove_child(self, c):
        assert isinstance(c, Node)
        self.csize -= c.size
        idx = self.child_keys.index(c.key)
        del self.children[idx]
        del self.child_keys[idx]
        if self.mark_dirty() and self.parent:
            self.parent.mark_dirty()

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
        idx = self._add_child(c)
        if not idx:
            self._update_key_maybe()
        if self.csize <= self.maximum_size:
            return
        _debug(' too big, splitting')
        tn = self.__class__()
        while self.csize > tn.csize:
            tn._add_child(self._pop_child(-1))
        tn.key = tn.child_keys[0]
        if self.parent is not None:
            _debug(' did not cause new root')
            return self.parent.add_child(tn)
        # We're root -> Add new level
        _debug(' caused new root')
        tn2 = self.__class__()
        self.key = self.child_keys[0]
        tn2._add_child(self)
        tn2._add_child(tn)
        return tn2

    def add(self, c):
        _debug('adding %s to %s', c, self)
        sc = self.search_prev_or_eq(c)
        if sc:
            assert sc.key != c.key
            _debug(' closest match: %s', sc)
            return sc.parent.add_child(c) or self

        return self.add_child(c) or self

    @property
    def children(self):
        return self._children

    @property
    def child_keys(self):
        return self._child_keys

    @property
    def depth(self):
        if self.is_leafy:
            return 1
        return self.children[0].depth + 1

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
        self._remove_child(c)
        if self.csize >= self.minimum_size:
            return

        def _equalize(sib, idx):
            if sib.csize >= self.has_spares_size:
                while sib.csize >= self.csize:
                    self._add_child(sib._pop_child(idx))
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
            sib._add_child(self._pop_child(0))

        sib._update_key_maybe()

        self.parent.remove_child(self)

    @lazy_property
    def size(self):
        return self.header_size + NAME_SIZE

    def remove(self, c):
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
            self._remove_child(c)
            for c2 in c.children:
                self._add_child(c2)

        # this is root so  no need to worry about .key handling..

    def search_prev_or_eq(self, c):
        _debug('search_prev_or_eq %s in %s', c, self)
        n = self
        k = c.key
        while True:
            if not n.child_keys:
                _debug(' no children')
                return
            _debug(' child_keys %s', n.child_keys)
            idx = bisect.bisect_right(n.child_keys, k)
            if idx:
                idx -= 1
            _debug(' idx %d', idx)
            n = n.children[idx]
            _debug(' = %s', n)
            if not isinstance(n, TreeNode):
                return n

    def search(self, c):
        sc = self.search_prev_or_eq(c)
        if sc and sc.key == c.key:
            return sc
