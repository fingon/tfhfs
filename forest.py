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
# Last modified: Tue Jun 28 13:38:30 2016 mstenber
# Edit time:     186 min
#
"""

This is the 'forest layer' main module.

It implements nested tree concept, with an interface to the storage layer.

"""

import bisect
import functools
import logging

_debug = logging.getLogger(__name__).debug

HASH_SIZE = 32  # bytes
NAME_HASH_SIZE = 4  # bytes
NAME_SIZE = 256  # maximum length of single name


@functools.total_ordering
class Node:

    header_size = 0

    parent = None

    def __init__(self, *, nwrites=0, lastwrite=0):
        d = locals().copy()
        del d['self']
        self.__dict__.update(d)

    @property
    def root(self):
        if self.parent is None:
            return self
        return self.parent.root

    def __eq__(self, other):
        if not isinstance(other, Node):
            return NotImplemented
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, Node):
            return NotImplemented
        return self.name < other.name

    @property
    def size(self):
        return (self.header_size + len(self.name) + HASH_SIZE +
                NAME_HASH_SIZE)


class LeafNode(Node):

    def __init__(self, name, **kw):
        assert isinstance(name, bytes)
        self.name = name
        Node.__init__(self, **kw)

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)


class TreeNode(Node):
    """ A class which represents a single intermediate or root tree
node. """

    maximum_size = 128000
    minimum_size = maximum_size * 1 / 4
    has_spares_size = maximum_size / 2

    # Note: minimum_size + has_spares_size MUST be less than maximum_size

    def __init__(self, **kw):
        Node.__init__(self, **kw)
        self.children = []
        self.csize = 0

    def __repr__(self):
        return '<%s >=%s - depth %d>' % (self.__class__.__name__,
                                         self.name,
                                         self.depth)

    def _add_child(self, c):
        assert isinstance(c, Node)
        self.csize += c.size
        #_debug(' children pre:%s', self.children)
        bisect.insort(self.children, c)
        #_debug(' children post:%s', self.children)
        c.parent = self

    def _pop_child(self, idx):
        c = self.children[idx]
        self._remove_child(c)
        return c

    def _remove_child(self, c):
        assert isinstance(c, Node)
        self.csize -= c.size
        self.children.remove(c)

    def add_child(self, c):
        _debug('add_child %s', c)
        self._add_child(c)
        if self.csize <= self.maximum_size:
            return
        _debug(' too big, splitting')
        tn = self.__class__()
        while self.csize > tn.csize:
            tn._add_child(self._pop_child(-1))
        if self.parent is not None:
            _debug(' did not cause new root')
            return self.parent.add_child(tn)
        # We're root -> Add new level
        _debug(' caused new root')
        tn2 = self.__class__()
        tn2._add_child(self)
        tn2._add_child(tn)
        return tn2

    def add(self, c):
        _debug('adding %s to %s', c, self)
        sc = self.search_prev_or_eq(c)
        if sc:
            assert sc != c
            _debug(' closest match: %s', sc)
            return sc.parent.add_child(c) or self
        assert not self.children
        return self.add_child(c) or self

    @property
    def depth(self):
        if self.is_leafy:
            return 1
        return self.children[0].depth + 1

    def get_smaller_sib(self):
        if not self.parent:
            return
        idx = self.parent.children.index(self) - 1
        if idx >= 0:
            return self.parent.children[idx]

    def get_larger_sib(self):
        if not self.parent:
            return
        idx = self.parent.children.index(self) + 1
        if idx < len(self.parent.children):
            return self.parent.children[idx]

    @property
    def is_leafy(self):
        return not self.children or not isinstance(self.children[0], TreeNode)

    def remove_child(self, c):
        self._remove_child(c)
        if self.csize >= self.minimum_size:
            return

        def _equalize(sib, idx):
            if sib.csize >= self.has_spares_size:
                while sib.csize >= self.csize:
                    self._add_child(sib._pop_child(idx))
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

        self.parent.remove_child(self)

    @property
    def name(self):
        return self.children and self.children[0].name

    @property
    def size(self):
        return (self.header_size + NAME_SIZE + HASH_SIZE + NAME_HASH_SIZE)

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

    def search_prev_or_eq(self, c):
        _debug('search_prev_or_eq %s in %s', c, self)
        n = self
        while True:
            if not n.children:
                _debug(' no children')
                return
            # Find the best child to add it to
            idx = bisect.bisect_right(n.children, c)
            if idx:
                idx -= 1
            n = n.children[idx]
            _debug(' idx %d = %s', idx, n)
            if not isinstance(n, TreeNode):
                return n

    def search(self, c):
        sc = self.search_prev_or_eq(c)
        if sc == c:
            return sc

    @classmethod
    def from_block(self, data):
        # TBD: use pycapnp to do stuff - http://jparyani.github.io/pycapnp/
        raise NotImplementedError

    def to_block(self):
        # TBD: use pycapnp to do stuff - http://jparyani.github.io/pycapnp/
        raise NotImplementedError
