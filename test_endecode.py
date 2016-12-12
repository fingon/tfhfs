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
# Last modified: Tue Dec 13 05:47:10 2016 mstenber
# Edit time:     11 min
#
"""

"""

import pytest

from endecode import Decoder, Encoder


@pytest.mark.parametrize('v', [0, 1, 3, 2362436622])
def test_uint64(v):
    v2 = Decoder(Encoder().encode_uint64(v).value).decode_uint64()
    assert v == v2


@pytest.mark.parametrize('v', [0, 1, 3, 236222])
def test_uint32(v):
    v2 = Decoder(Encoder().encode_uint32(v).value).decode_uint32()
    assert v == v2


@pytest.mark.parametrize('v', [-1234678458, -73, -1, 0, 1, 3, 2362436])
def test_int(v):
    v2 = Decoder(Encoder().encode_int(v).value).decode_int()
    assert v == v2


@pytest.mark.parametrize('v', [b'foo', b''])
def test_cbytes(v):
    v2 = Decoder(Encoder().encode_cbytes(v).value).decode_cbytes()
    assert v == v2


@pytest.mark.parametrize('v', [b'foob', b'foo', b'fo', b'f', b''])
def test_b64(v):
    v2 = Decoder(Encoder().encode_base64url(v).value).decode_base64url()
    assert v == v2
