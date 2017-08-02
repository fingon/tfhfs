#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- Python -*-
#
# $Id: perf_fs.py $
#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2016 Markus Stenberg
#
# Created:       Sun Dec 25 08:04:44 2016 mstenber
# Last modified: Wed Aug  2 11:49:36 2017 mstenber
# Edit time:     71 min
#
"""

This is 'whole'-system benchmark used to gather data for populating
the 'official' performance figures with.

"""

import argparse
import os
import os.path
import subprocess

_support_dir = os.path.dirname(__file__)
_test_fs = os.path.join(_support_dir, '..', 'test_fs.py')


def open_fs(args):
    return (subprocess.Popen([_test_fs] + list(args), stdout=2),
            args)


def close_fs(t):
    import test_fs

    (p, args) = t
    args = test_fs.argument_parser().parse_args(args)
    try:
        subprocess.call(['umount', args.mountpoint], stderr=subprocess.DEVNULL)
    except:
        pass
    try:
        rc = p.wait(timeout=3)
    except subprocess.TimeoutExpired:
        p.terminate()
        try:
            rc = p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


if __name__ == '__main__':

    global __package__
    if __package__ is None:
        import python3fuckup
        __package__ = python3fuckup.get_package(__file__, 1)

    import time

    read_cmd = 'find /tmp/x -type f | xargs cat > /dev/null'
    tests = []
    # tests.append(('In-memory dict', None, '', [])), # n/a really
    if True:
        tests.extend([
            ('SQLite compressed+encrypted',
             'sqlite', '/tmp/foo', ['-c', '-p', '/Users/mstenber/bin/insecurepassword']),
            ('SQLite encrypted',
             'sqlite', '/tmp/foo', ['-p', '/Users/mstenber/bin/insecurepassword']),
            ('SQLite',
             'sqlite', '/tmp/foo', [])
        ])
    if False:
        # inexplicably lmdb is actually slower than sqlite!
        tests.extend([
            ('Lmdb compressed+encrypted',
             'lmdb', '/tmp/foo2', ['-b', 'lmdb', '-c', '-p', '/Users/mstenber/bin/insecurepassword']),
            ('Lmdb encrypted',
             'lmdb', '/tmp/foo2', ['-b', 'lmdb', '-p', '/Users/mstenber/bin/insecurepassword']),
            ('Lmdb',
             'lmdb', '/tmp/foo2', ['-b', 'lmdb']),
        ])

    p = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('--test', '-t', type=int, default=-1,
                   help='Run single test (-1 = all)')
    args = p.parse_args()
    if args.test >= 0:
        tests = [tests[args.test]]
    for desc, backend_type, backend, backend_options in tests:
        print(f'# {desc}')
        for write_cmd, units, unit_type in [
                ('dd "if=/Volumes/ulko/share/2/software/unix/2015-09-24-raspbian-jessie.img" of=/tmp/x/foo.dat bs=1024000',
                 4325, 'megabyte'),  # 1 file :p
                ('rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/',
                 56711, 'file'),  # 1082MB
        ]:
            print(f'## Write {units} {unit_type}s')

            print(f'Command: {write_cmd}')
            if backend:
                if os.path.isfile(backend):
                    os.unlink(backend)
                elif os.path.isdir(backend):
                    os.system('rm -rf %s' % backend)
            args = []
            if backend:
                args.extend(['-f', backend])
                args.extend(backend_options)
            t = open_fs(args)
            time.sleep(1)
            start_time = time.time()
            os.system(write_cmd)
            close_fs(t)
            write_time = time.time() - start_time
            cnt = units // write_time
            print()
            print(f'Took {write_time} seconds')
            print(f'{cnt} {unit_type}s per second')
            print()

            if backend:
                print(f'## Read it back')
                print(f'Command: {read_cmd}')
                t = open_fs(args)
                time.sleep(1)
                start_time = time.time()
                os.system(read_cmd)
                close_fs(t)
                read_time = time.time() - start_time
                cnt = units // read_time
                print()
                print(f'Took {read_time} seconds')
                print(f'{cnt} {unit_type}s per second')
                print()
