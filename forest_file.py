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
# Last modified: Tue Dec 13 20:43:51 2016 mstenber
# Edit time:     24 min
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

import os

import inode


class FDStore:

    def __init__(self):
        self.fd2o = {}
        self.freefds = []

    def allocate_fd(self):
        if not self.fd2o:
            fd = 1
        elif self.freefds:
            fd = self.freefds.pop(0)
        else:
            fd = len(self.fd2o) + 1
        assert fd not in self.fd2o
        self.fd2o[fd] = None
        return fd

    def lookup_fd(self, fd):
        return self.fd2o[fd]

    def register_fd(self, fd, o):
        assert o
        assert self.fd2o[fd] is None
        self.fd2o[fd] = o

    def unregister_fd(self, fd):
        self.freefds.append(fd)
        del self.fd2o[fd]


class FileDescriptor:
    inode = None

    def __init__(self, fd, inode, flags):
        assert inode
        self.fd = fd
        self.inode = inode
        self.flags = flags
        self.inode.ref()

    def __del__(self):
        self.close()

    def close(self):
        if self.inode is None:
            return
        self.inode.store.unregister_fd(self.fd)
        self.inode.deref()
        del self.inode

    def dup(self):
        return self.inode.open(self.flags)

    def read(self, ofs, size):
        pass

    def write(self, ofs, buf):
        # TBD: really implement this
        return len(buf)


class FileINode(inode.INode):

    def open(self, flags):
        assert isinstance(self.store, FDStore)
        if flags & os.O_TRUNC:
            self.trunc()
        fd = self.store.allocate_fd()
        o = FileDescriptor(fd, self, flags)
        self.store.register_fd(fd, o)
        return fd

    def set_size(self, n):
        pass

    def trunc(self):
        self.set_size(0)
