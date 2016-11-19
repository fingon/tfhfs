#
# $Id: Makefile $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Nov 19 10:48:22 2016 mstenber
# Last modified: Sat Nov 19 10:49:32 2016 mstenber
# Edit time:     1 min
#
#

# Utility to get number of cores; I am not sure I really want to run
# anything else than OS X or Linux anyway.
ifeq ($(shell uname),Darwin)
	CORE_COUNT=`sysctl machdep.cpu.core_count | cut -d ':' -f 2`
else
	CORE_COUNT=`nproc`
endif

# Set this to blank if you want pip requirements to non-user ones
# (I CBA with venv for this for now)
PIP_TO_USER=--user

all: test

clean:
	rm -f .done.*

test: .done.requirements
	py.test -n $(CORE_COUNT)

.done.requirements:
	pip3 install $(PIP_TO_USER) -r requirements.txt
	touch $@
