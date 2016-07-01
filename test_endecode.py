#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: test_endecode.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Thu Jun 30 17:09:11 2016 mstenber
# Last modified: Fri Jul  1 12:22:00 2016 mstenber
# Edit time:     7 min
#
"""

"""

import unittest

from ddt import data, ddt, unpack

from endecode import Decoder, Encoder


@ddt
class EnDecodeTests(unittest.TestCase):

    @data(0, 1, 3, 2362436622)
    def test_uint64(self, v):
        v2 = Decoder(Encoder().encode_uint64(v).value).decode_uint64()
        assert v == v2

    @data(0, 1, 3, 236222)
    def test_uint32(self, v):
        v2 = Decoder(Encoder().encode_uint32(v).value).decode_uint32()
        assert v == v2

    @data(-1234678458, -73, -1, 0, 1, 3, 2362436)
    def test_int(self, v):
        v2 = Decoder(Encoder().encode_int(v).value).decode_int()
        assert v == v2

    @data(-1234678458, -73, -1, 0, 1, 3, 2362436)
    def test_int(self, v):
        v2 = Decoder(Encoder().encode_int(v).value).decode_int()
        assert v == v2

    @data(b'foo', b'')
    def test_cbytes(self, v):
        v2 = Decoder(Encoder().encode_cbytes(v).value).decode_cbytes()

    @data(b'foob', b'foo', b'fo', b'f', b'')
    def test_b64(self, v):
        v2 = Decoder(Encoder().encode_base64url(v).value).decode_base64url()
