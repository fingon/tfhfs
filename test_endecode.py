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
# Last modified: Thu Jun 30 17:12:54 2016 mstenber
# Edit time:     2 min
#
"""

"""

import unittest

from ddt import data, ddt, unpack

from endecode import Decoder, Encoder


@ddt
class EnDecodeTests(unittest.TestCase):

    @data(-1234678458, -73, -1, 0, 1, 3, 2362436)
    def test_int(self, v):
        v2 = Decoder(Encoder().encode_int(v).get_result()).decode_int()
        assert v == v2

    @data(b'foo', b'')
    def test_cbytes(self, v):
        v2 = Decoder(Encoder().encode_cbytes(v).get_result()).decode_cbytes()
