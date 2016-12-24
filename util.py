#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: util.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Fri Nov 25 15:06:01 2016 mstenber
# Last modified: Sat Dec 24 06:39:46 2016 mstenber
# Edit time:     60 min
#
"""

Random utility things.

"""

import collections
import hashlib
import logging
import sys

import cbor

from ms.lazy import lazy_property

_debug = logging.getLogger(__name__).debug


def sha256(*l):
    """Convenience method to sha256 bunch of int/bytes.

    Integers are converted to bytes (TBD: should 'larger' ints be
    supported? in practise we want just type info which should be <=
    byte, and CBOR encoded raw bytes)
    """
    h = hashlib.sha256()
    for s in l:
        if isinstance(s, int):
            s = bytes([s])
        h.update(s)
    return h.digest()


class Allocator:
    """Utility which can be used to provide int <-> object mapping.

    Notably, used for both file descriptors and inodes.

    The underlying assumption is that we have a Python object that we
    _really_ want to use, and do not really need to store anything in
    the object itself.
    """

    def __init__(self, start_value=1):
        self.start_value = start_value
        self.value2object = {}
        self.object2value = {}
        self.free_values = []

    def _allocate(self):
        if self.free_values:
            v = self.free_values.pop(0)
            _debug('reused free v #%d', v)
        else:
            v = len(self.value2object) + self.start_value
            _debug('allocated new v #%d', v)
        assert v not in self.value2object
        self.value2object[v] = None
        return v

    def count(self):
        return len(self.value2object)

    def get_by_value(self, value):
        assert isinstance(value, int)
        return self.value2object[value]

    def getdefault_by_value(self, value, default=None):
        return self.value2object.get(value, default)

    def get_value_by_object(self, object):
        return self.object2value[object]

    @property
    def max_value(self):
        return self.start_value + len(self.free_values) + len(self.value2object)

    def register(self, o):
        v = self._allocate()
        assert self.value2object[v] is None
        assert o not in self.object2value
        self.value2object[v] = o
        self.object2value[o] = v
        return v

    def unregister(self, o):
        v = self.get_value_by_object(o)
        assert o is not None
        del self.value2object[v]
        del self.object2value[o]
        self.free_values.append(v)


class DirtyMixin:
    """Provide dirtiness tracking.

    Notably, mark_dirty() to mark dirty the node, and related nodes
    (if any in the mark_dirty_related implemented by a subclass).

    flush() and subclass responsibility perform_flush() handles
    flushing of dirty data.

    """

    dirty = False
    parent = None

    def mark_dirty(self):
        if self.dirty:
            _debug('%s already dirty', self)
            return
        _debug('marked dirty: %s', self)
        self.dirty = True
        self.mark_dirty_related()
        return True

    def mark_dirty_related(self):
        if self.parent:
            self.parent.mark_dirty()

    def flush(self):
        if not self.dirty:
            return
        del self.dirty
        return self.perform_flush()

    def perform_flush(self):
        raise NotImplementedError


class DataMixin(DirtyMixin):
    """Provide an arbitrary dict's worth of data.

    Instead of storing the dict locally, it is accessed via .data
    property of the object for reading, and set_data(k,v) is used to
    set values, which triggers dirtiness handling.
    """

    _data = None

    @property
    def data(self):
        if self._data is None:
            self._data = {}
        return self._data

    @property
    def nonempty_data(self):
        return {k: v for k, v in self.data.items() if v}

    def set_data(self, k, v):
        assert (not self.cbor_data_pickler
                or k in self.cbor_data_pickler.internal2external_dict)
        data = self.data
        if data.get(k) == v:
            _debug('%s redundant set_data %s=%s', self, k, v)
            return
        data[k] = v
        self.mark_dirty()

    cbor_data_pickler = None

    @property
    def _cbor_data(self):
        return dict(self.cbor_data_pickler.get_external_items_from_internal_items(self.data.items()))

    @_cbor_data.setter
    def _cbor_data(self, value):
        d = dict(
            self.cbor_data_pickler.get_internal_items_from_external_items(value.items()))
        self._data = d


class CBORPickler:
    """Convenience method for storing instance method data in CBOR.

    Typically there is only few instances of this per class, and
    therefore the amount of per-instance state is not really worth
    minimizing.
    """

    def __init__(self, internal2external_dict):
        """
        Constructor;

        internal2external_dict: The internal key -> CBOR key mapping.

        For CBOR, e.g. integer keys are efficient, or one-letter ones.
        """
        self.internal2external_dict = internal2external_dict

    @lazy_property
    def external2internal_dict(self):
        d = {v: k for k, v in self.internal2external_dict.items()}
        return d

    def dumps(self, o):
        return cbor.dumps(self.get_external_dict(o))

    def get_internal_dict_items(self, o):
        for k in self.internal2external_dict.keys():
            v = getattr(o, k, None)
            # If it is class default, no point dumping
            # if not provided by class, we assume default to be None
            if v == getattr(o.__class__, k, None):
                continue
            yield k, v

    def get_external_items_from_internal_items(self, it):
        for k, v in it:
            yield self.internal2external_dict[k], v

    def get_external_dict(self, o):
        return dict(self.get_external_items_from_internal_items(self.get_internal_dict_items(o)))

    def get_internal_items_from_external_items(self, it):
        for k, v in it:
            yield self.external2internal_dict[k], v

    def load_external_dict_to(self, d, o):
        self.set_external_dict_to(cbor.loads(d), o)
        return o

    def set_external_dict_to(self, d, o):
        self.unload_from(o)
        for k, v in self.get_internal_items_from_external_items(d.items()):
            setattr(o, k, v)

    def unload_from(self, o):
        for k in self.external2internal_dict.values():
            if hasattr(o, k):
                try:
                    delattr(o, k)
                except AttributeError:
                    # may be in class..
                    pass


def getrecsizeof(o, seen=None):
    if seen is None:
        seen = set()
    if id(o) in seen:
        return 0
    seen.add(id(o))
    c = sys.getsizeof(o)
    if isinstance(o, dict):
        for k, v in o.items():
            c += getrecsizeof(k, seen)
            c += getrecsizeof(v, seen)
    elif isinstance(o, str) or isinstance(o, bytes):
        pass
    elif isinstance(o, collections.Iterable):
        for e in o:
            c += getrecsizeof(e, seen)
    return c


def to_bytes(s):
    if isinstance(s, str):
        return s.encode()
    assert isinstance(s, bytes)
    return s


def zeropad_bytes(size, s=b''):
    assert isinstance(size, int)
    assert isinstance(s, bytes)
    topad = size - len(s)
    if topad > 0:
        s = s + bytes([0] * topad)
    return s
