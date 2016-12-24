#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: perf_crypto.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Wed Jun 29 09:10:49 2016 mstenber
# Last modified: Sat Dec 24 17:01:02 2016 mstenber
# Edit time:     52 min
#
"""Test performance of various things related to confidentiality and
authentication.

Results from nMP 29.6.2016:

mmh3 10               : 1978889.2211/sec [100ms] (0.5053us/call)
mmh3 100k             :   63844.0223/sec [97.9ms] (15.663us/call)
aes 10                :   27812.2448/sec [97.5ms] (35.955us/call)
aes 100k              :    4038.9085/sec [97.6ms] (247.59us/call)
aes gcm 10            :   33470.4986/sec [97.2ms] (29.877us/call)
aes gcm 100k          :    7855.5295/sec [98.4ms] (127.3us/call)
aes gcm full 10       :   22897.1457/sec [98.9ms] (43.674us/call)
aes gcm full 100k     :    7050.3911/sec [98.7ms] (141.84us/call)
sha 256 10            :   63235.0453/sec [97.4ms] (15.814us/call)
sha 256 100k          :    3098.3940/sec [98.8ms] (322.75us/call)
sha 256 (hashlib) 10  :  803653.0822/sec [97ms] (1.2443us/call)
sha 256 (hashlib) 100k:    3681.0414/sec [98.9ms] (271.66us/call)
sha 512 10            :   61342.3456/sec [101ms] (16.302us/call)
sha 512 100k          :    3109.7485/sec [97.8ms] (321.57us/call)
fernet 10             :   13287.5653/sec [97ms] (75.258us/call)
fernet 100k           :    1189.3112/sec [95.9ms] (840.82us/call)

=> Fernet seems insanely slow, aes gcm is the winner for simple
conf+auth, and raw sha256 seems to work fine for what we want to do
(300+MB/s on single core). 32-bit Murmurhash3 is virtually free (6
GB/s on single core).

As a matter of fact, if we want to ensure 'correct' data coming out,
aes gcm is cheaper check than sha256 of the data! Therefore, ENCRYPT
EVERYTHING! MU HA HA..

When saving, we obviously want to still use SHA256 hash, but for
loading, we can simply include the hash of plaintext _within_ AES GCM
envelope and therefore verify it that way so loading will be 2x as
fast with AES GCM scheme than plaintext + SHA256. Hardware-accelerated
cryptography is like magic..

"""

import base64
import hashlib
import os

from cryptography.fernet import Fernet  # pip install cryptography
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import cmac, hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

import mmh3  # pip install murmurhash3
import ms.perf

text10 = b'1234567890'
text100k = text10 * 10000
assert len(text100k) == 100000


def test_mmh(s):
    mmh3.hash64(s)


password = b'password'
salt = os.urandom(16)
kdf = PBKDF2HMAC(algorithm=hashes.SHA256(),
                 length=32,
                 salt=salt,
                 iterations=100000,
                 backend=default_backend())
rawkey = kdf.derive(password)
key = base64.urlsafe_b64encode(rawkey)
f = Fernet(key)

_sha256 = hashes.SHA256()
_default_backend = default_backend()


def test_sha256(s):
    d = hashes.Hash(_sha256, backend=_default_backend)
    d.update(s)
    d.finalize()


def test_hashlib_sha256(s):
    h = hashlib.sha256()
    h.update(s)
    h.digest()

_sha512 = hashes.SHA256()


def test_sha512(s):
    d = hashes.Hash(_sha256, backend=_default_backend)
    d.update(s)
    d.finalize()

iv = os.urandom(16)
cipher = Cipher(algorithms.AES(rawkey), modes.CBC(iv),
                backend=_default_backend)


def test_aes(s):
    encryptor = cipher.encryptor()
    padder = padding.PKCS7(128).padder()
    s = padder.update(s) + padder.finalize()
    r = encryptor.update(s) + encryptor.finalize()

cipher_gcm = Cipher(algorithms.AES(rawkey), modes.GCM(iv),
                    backend=_default_backend)


aes_cmac = cmac.CMAC(algorithms.AES(rawkey), backend=_default_backend)
assert len(rawkey) == 32


def test_aes_cmac(s):
    c = aes_cmac.copy()
    c.update(s)
    d = c.finalize()
    assert len(d) == 16


def test_aes_gcm(s):
    encryptor = cipher_gcm.encryptor()
    r = encryptor.update(s) + encryptor.finalize()


def test_aes_gcm_full(s):
    iv_new = os.urandom(16)
    cipher_gcm_full = Cipher(algorithms.AES(rawkey), modes.GCM(iv_new),
                             backend=_default_backend)
    encryptor = cipher_gcm_full.encryptor()
    r = encryptor.update(s) + encryptor.finalize()


def test_fernet(s):
    f.encrypt(s)


l = []
for (label, fun) in [('mmh3', test_mmh),
                     ('aes', test_aes),
                     ('aes cmac', test_aes_cmac),
                     ('aes gcm', test_aes_gcm),
                     ('aes gcm full', test_aes_gcm_full),
                     ('sha 256', test_sha256),
                     ('sha 256 (hashlib)', test_hashlib_sha256),
                     ('sha 512', test_sha512),
                     ('fernet', test_fernet)]:
    def _foo1(fun=fun):
        fun(text10)

    def _foo2(fun=fun):
        fun(text100k)

    l.append(('%s 10' % label, _foo1))
    l.append(('%s 100k' % label, _foo2))

ms.perf.testList(l, maxtime=0.1)
