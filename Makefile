#
# $Id: Makefile $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sat Nov 19 10:48:22 2016 mstenber
# Last modified: Fri Sep 15 11:01:06 2017 mstenber
# Edit time:     53 min
#
#

# Which Python3 to use
PYTHON3=python3
PYTHON3=pypy3

PIP3=pip3
PIP3=pip_pypy3

# Utility to get number of cores; I am not sure I really want to run
# anything else than OS X or Linux anyway.
ifeq ($(shell uname),Darwin)
       CORE_COUNT=`sysctl machdep.cpu.core_count | cut -d ':' -f 2`
# ^ core_count is real cores, and not hyperthreads -> faster with fewer, yay.
else
       CORE_COUNT=auto
endif

 # Set this to blank if you want

# These are somewhat OS X driven
PIP3_CFLAGS=-I/usr/local/opt/openssl/include
PIP3_LDFLAGS='-undefined dynamic_lookup -L/usr/local/opt/openssl/lib'


# Set this to blank if you want pip requirements to non-user ones
# (I CBA with venv for this for now)
PIP3_TO_USER=--user

all: test

# autopep8
# ^ no longer really relevant due to my Emacs default config
# autopep8/isorting all Python code I touch..


clean:
	rm -f .done.*

# -rx = extra detail about xfails
# -rw = extra detail about pytest warnings
# --strict = warning = error
# -o xfail_strict=True = xpass = fail as well

PYTEST_ARGS=--strict -o xfail_strict=True -n $(CORE_COUNT)
PYTEST=$(PYTHON3) `which pytest`

autopep8: .done.autopep8

test: .done.requirements
	$(PYTEST) --no-print-logs -rx -rw $(PYTEST_ARGS)

cov: .done.coverage
	open htmlcov/index.html

log.txt: .done.requirements $(wildcard *.py)
	$(PYTEST) -p no:sugar  $(PYTEST_ARGS) -rx -rw 2>&1 | tee log.txt

profile: .done.profile

perf.md: .done.perf
	cp .done.perf $@

pstats: profile
	python3 -c 'import pstats;pstats.Stats(".done.profile").sort_stats("cumtime").print_stats(100)' | egrep -v '/(Cellar|site-packages)/' | egrep -v '(<frozen|{built-in)'

.done.coverage: $(wildcard *.py)
	rm -rf htmlcov
	$(PYTEST) --cov-report=html --cov=. $(PYTEST_ARGS)
	touch $@

.done.perf: $(wildcard *.py)
	$(PYTHON3) support/perf_fs.py | tee $@.new
	mv $@.new $@

.done.profile: .done.requirements $(wildcard *.py)
	$(PYTHON3) -m cProfile -o $@.new `which py.test`
	mv $@.new $@

.done.requirements: requirements/*.txt
	CFLAGS=$(PIP3_CFLAGS) LDFLAGS=$(PIP3_LDFLAGS) $(PIP3) \
		install --upgrade $(PIP3_TO_USER) \
		-c requirements/constraints.txt -r requirements/runtime.txt \
		-r requirements/development.txt
	touch $@


.done.autopep8: $(wildcard *.py)
	autopep8 --in-place *.py
	touch $@
