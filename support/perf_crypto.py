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
# Last modified: Wed Jun 29 11:27:28 2016 mstenber
# Edit time:     39 min
#
"""Test performance of various things related to confidentiality and
authentication.

Results from nMP 29.6.2016:

mmh3 10          : 2133247.9326/sec [101ms] (0.4688us/call)
mmh3 100k        :   63429.9474/sec [98.4ms] (15.765us/call)
aes 10           :   22198.3661/sec [97.7ms] (45.048us/call)
aes 100k         :    3784.7220/sec [98.6ms] (264.22us/call)
aes gcm 10       :   21201.4869/sec [99.7ms] (47.167us/call)
aes gcm 100k     :    7043.6806/sec [93.8ms] (141.97us/call)
aes gcm full 10  :   16539.1683/sec [96.3ms] (60.463us/call)
aes gcm full 100k:    6467.6065/sec [98ms] (154.62us/call)
sha 256 10       :   48997.3152/sec [98.6ms] (20.409us/call)
sha 256 100k     :    3369.2205/sec [98.5ms] (296.8us/call)
fernet 10        :   10607.2052/sec [89.3ms] (94.276us/call)
fernet 100k      :    1185.1519/sec [97ms] (843.77us/call)

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

import mmh3  # pip install murmurhash3
import ms.perf
import os

from cryptography.fernet import Fernet  # pip install cryptography
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
from cryptography.hazmat.primitives import padding

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
                     ('aes gcm', test_aes_gcm),
                     ('aes gcm full', test_aes_gcm_full),
                     ('sha 256', test_sha256),
                     ('sha 512', test_sha512),
                     ('fernet', test_fernet)]:
    def _foo1(fun=fun):
        fun(text10)

    def _foo2(fun=fun):
        fun(text100k)

    l.append(('%s 10' % label, _foo1))
    l.append(('%s 100k' % label, _foo2))

ms.perf.testList(l, maxtime=0.1)
