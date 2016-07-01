#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: endecode.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Thu Jun 30 16:53:21 2016 mstenber
# Last modified: Fri Jul  1 12:21:55 2016 mstenber
# Edit time:     27 min
#
"""Encoder/Decoder for binary data.

When we're dealing per-value, the 'benefit' of using e.g. struct.X
rapidly vanishes. This leads to readable code at least which is nice.

The number of datatypes done in this codebase is rather small, but as
their sizes are mostly dynamic, this is better fit than
e.g. pybabel-like TLV magic format, or some other endecode library.

"""

import base64

# base64 altchars to use; the default +/ is not good, as / is bad
# within (non-path-containing) filenames. -_ is used in e.g. RFC4648
# base64url.
_altchars = b'-_'


class Decoder:

    def __init__(self, b):
        self.ofs = 0
        self.b = b

    def _left(self):
        return len(self.b) - self.ofs

    def decode_base64url(self):
        b = self.decode_bytes_rest()
        b += b'=' * (-len(b) % 4)
        return base64.b64decode(b, altchars=_altchars, validate=True)

    def decode_bytes(self, n):
        assert self._left() >= n
        d = self.b[self.ofs:self.ofs + n]
        self.ofs += n
        return d

    def decode_bytes_rest(self):
        return self.decode_bytes(self._left())

    def decode_cbytes(self):
        n = self.decode_uint()
        return self.decode_bytes(n)

    def decode_uint8(self):
        b = self.decode_bytes(1)
        return b[0]

    def decode_uint32(self):
        b1 = self.decode_uint8()
        b2 = self.decode_uint8()
        b3 = self.decode_uint8()
        b4 = self.decode_uint8()
        return b1 << 24 | b2 << 16 | b3 << 8 | b4

    def decode_uint64(self):
        i1 = self.decode_uint32()
        i2 = self.decode_uint32()
        return i1 << 32 | i2

    def decode_uint(self):
        v = 0
        b = self.decode_uint8()
        while b & 0x80:
            v = v << 7 | b & 0x7F
            b = self.decode_uint8()
        v = v << 7 | b
        return v

    def decode_int(self):
        v = self.decode_uint()
        if v % 2 == 0:
            return -v / 2
        return v // 2 + 1


class Encoder:

    def __init__(self):
        self.l = []

    def encode_base64url(self, b):
        self.l.append(base64.b64encode(b, altchars=_altchars).rstrip(b'='))
        return self

    def encode_bytes(self, b):
        self.l.append(b)
        return self

    def encode_cbytes(self, b):
        self.encode_uint(len(b))
        self.encode_bytes(b)
        return self

    def encode_uint8(self, v):
        assert isinstance(v, int) and v >= 0 and v < 256
        self.encode_bytes(bytes([v]))
        return self

    def encode_uint32(self, v):
        self.encode_uint8(v >> 24)
        self.encode_uint8(v >> 16 & 0xff)
        self.encode_uint8(v >> 8 & 0xff)
        self.encode_uint8(v & 0xff)
        return self

    def encode_uint64(self, v):
        self.encode_uint32(v >> 32)
        self.encode_uint32(v & 0xFFFFFFFF)
        return self

    def encode_uint(self, v):
        i = 0
        tv = v
        while tv >= 0x80:
            tv = tv >> 7
            i += 1
        for j in range(i, 0, -1):
            self.encode_uint8(((v >> (j * 7)) & 0x7f) | 0x80)
        self.encode_uint8(v & 0x7f)
        return self

    def encode_int(self, v):
        if v <= 0:
            self.encode_uint((-v * 2))
        else:
            self.encode_uint(v * 2 - 1)
        return self

    @property
    def value(self):
        return b''.join(self.l)
