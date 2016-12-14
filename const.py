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
# Last modified: Thu Dec 15 08:05:42 2016 mstenber
# Edit time:     8 min
#
"""

"""


# 'type' of a (typed) block.

TYPE_DIRNODE = 1
# children are also DirectoryTreeNodes / leaf children are DirectoryEntries

TYPE_FILENODE = 2
# children are also FileTreeNodes / leaf children are FileData

TYPE_FILEDATA = 3  # node itself is FileData

TYPE_MASK = 0xF

BIT_WEAK = 0x20  # weak block -> no references
# TBD: Is this needed? Possibly for the non-full client case?

BIT_LEAFY = 0x40  # children are leaves of base type

BIT_COMPRESSED = 0x80  # compression was applied to the block

DENTRY_MODE_DIR = 0o1000
DENTRY_MODE_MINIFILE = 0o2000  # single data block; no underlying tree

# how much data we intern inside DirectoryEntries
INTERNED_BLOCK_DATA_SIZE_LIMIT = 128

# how large blocks we want to have (=normal FileData block maximum size)
BLOCK_SIZE_LIMIT = 128000

# llfuse.EntryAttributes reflection stuff
ATTR_STAT_KEYS = ['st_atime_ns', 'st_blksize', 'st_blocks', 'st_ctime_ns',
                  'st_gid', 'st_ino', 'st_mode',
                  'st_mtime_ns', 'st_nlink',
                  'st_rdev', 'st_size', 'st_uid']
ATTR_KEYS = ['attr_timeout', 'entry_timeout', 'generation'] + ATTR_STAT_KEYS
