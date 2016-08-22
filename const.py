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
# Last modified: Mon Aug 22 19:09:11 2016 mstenber
# Edit time:     3 min
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
