#
# $Id: Makefile $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Nov 19 10:48:22 2016 mstenber
# Last modified: Thu Dec 15 15:47:55 2016 mstenber
# Edit time:     10 min
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

# -rx = extra detail about xfails
# -rw = extra detail about pytest warnings
# --strict = warning = error
# -o xfail_strict=True = xpass = fail as well

test: .done.requirements
	py.test --strict -rx -rw -o xfail_strict=True -n $(CORE_COUNT)

.done.requirements: requirements/*.txt
	pip3 install --upgrade $(PIP_TO_USER) -c requirements/constraints.txt -r requirements/runtime.txt -r requirements/development.txt
	touch $@
