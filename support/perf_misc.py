#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: perf_misc.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Thu Jun 30 12:12:15 2016 mstenber
# Last modified: Thu Jun 30 12:14:09 2016 mstenber
# Edit time:     1 min
#
"""

nop      : 9020185.0387/sec [104ms] (0.1109us/call)
time.time: 9615391.5196/sec [126ms] (0.104us/call)

"""

import time
import ms.perf


def test_nop():
    pass

ms.perf.testList([['nop', test_nop],
                  ['time.time', time.time],
                  ], maxtime=0.1)
