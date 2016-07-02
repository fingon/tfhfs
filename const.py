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
# Last modified: Sat Jul  2 21:21:04 2016 mstenber
# Edit time:     2 min
#
"""

"""


# 'type' of a (typed) block.

TYPE_DIRNODE = 1
# children are also DirectoryTreeNodes / children are DirectoryEntries (l.)

TYPE_FILENODE = 2
# children are also FileTreeNodes / children are FileData (l.)

TYPE_FILEDATA = 3  # node itself is FileData

TYPE_MASK = 0xF

BIT_WEAK = 0x20  # weak block -> no references

BIT_LEAFY = 0x40  # children are leaves of base type

BIT_COMPRESSED = 0x80  # compression was applied to the block
