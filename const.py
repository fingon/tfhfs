#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: const.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Jul  2 21:10:04 2016 mstenber
# Last modified: Fri Jan 13 13:29:17 2017 mstenber
# Edit time:     22 min
#
"""

"""

import stat

# external information about block (not encoded inside block):
# - refcnt (number)
# - BLOCK_TYPE

BLOCK_TYPE_NORMAL = 1  # has references reflected in the system, has data
BLOCK_TYPE_MISSING = 2  # has references reflected in the system, has no data
BLOCK_TYPE_WEAK = 3  # has no references reflected in the system, has data


# 'type' of a (typed) block.

TYPE_DIRNODE = 1
# children are also DirectoryTreeNodes / leaf children are DirectoryEntries

TYPE_FILENODE = 2
# children are also FileTreeNodes / leaf children are FileData

TYPE_FILEDATA = 3  # node itself is FileData


TYPE_MASK = 0xF

BIT_LEAFY = 0x40  # children are leaves of base type

BIT_COMPRESSED = 0x80  # compression was applied to the block

# how much data we intern inside DirectoryEntries
INTERNED_BLOCK_DATA_SIZE_LIMIT = 128

# how large blocks we want to have (=normal FileData block maximum size)
BLOCK_SIZE_LIMIT = 128000

# llfuse.EntryAttributes reflection stuff
ATTR_STAT_KEYS = ['st_atime_ns',
                  # ^ _lazily_ provided by us (only updated on related writes)
                  'st_blksize',
                  # ^ efficient block size (Linuxism?)
                  'st_blocks',
                  # ^ Linuxisms, number of 512-byte blocks
                  'st_ctime_ns', 'st_mtime_ns',
                  # ^ timestamps, we should maintain them
                  'st_mode', 'st_gid', 'st_uid',
                  # ^ permission handling, should be used
                  'st_ino',
                  # ^ dynamic
                  'st_nlink',
                  # ^ always 1? can maybe just ignore?
                  'st_rdev',
                  # ^ can maybe just ignore?
                  'st_size',
                  # ^ files should do it automatically on flush
                  ]
ATTR_KEYS = ['attr_timeout', 'entry_timeout', 'generation',
             # these are all n/a, we do not care
             ] + ATTR_STAT_KEYS

FS_ROOT_MODE = 0o777 | stat.S_IFDIR
FS_ROOT_UID = 0
