#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: forest_file.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Dec  3 17:50:30 2016 mstenber
# Last modified: Sat Dec  3 18:21:19 2016 mstenber
# Edit time:     5 min
#
"""This is the file abstraction which is an INode subclass.

It wraps the lifecycle of a node in terms of different sizes;

- the minimal one with simply small amount of embedded data,

- single-block medium sized file, and

- large file which consists of a tree of data blocks.

The file inodes transition about these different size modes
transparently to the user.

The 'leaf' node (that is leaf node in the supertree) is where the file
is anchored. The content reading is done by accessing the existing
leaf (small file), raw data (medium file), or the tree (large
file). Writes cause transition between tree modes.

Growth:
 small -> medium: clear block_data, refer to explicit block with the data
 medium -> large: add tree with the medium block as its first (and only) block.

Shrinking:
 large -> medium: if only one block left, collapse the tree
 medium -> small: small remove block_id, set block_data

"""

import inode


class FileINode(inode.INode):
    pass
