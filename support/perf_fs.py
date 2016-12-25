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
# Last modified: Sun Dec 25 10:17:10 2016 mstenber
# Edit time:     55 min
#
"""

This is 'whole'-system benchmark used to gather data for populating
the 'official' performance figures with.

"""

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
    for desc, backend, backend_options in [
            ('In-memory dict', '', []),
            ('SQLite compressed+encrypted',
             '/tmp/foo', ['-c', '-p', '/Users/mstenber/bin/insecurepassword']),
            ('SQLite encrypted',
             '/tmp/foo', ['-p', '/Users/mstenber/bin/insecurepassword']),
            ('SQLite',
             '/tmp/foo', []),
    ]:
        print(f'# {desc}')
        for write_cmd, units, unit_type in [
                ('dd "if=/Users/mstenber/software/mac/10-11-elcapitan/Install OS X El Capitan.app/Contents/SharedSupport/InstallESD.dmg" of=/tmp/x/foo.dat bs=1024000', 5800, 'megabyte'),  # 1 file :p
                ('rsync -a /Users/mstenber/share/1/Maildir/.Junk /tmp/x/',
                 52122, 'file'),  # 942MB
        ]:
            print(f'## Write {units} {unit_type}s')

            print(f'Command: {write_cmd}')
            if backend:
                if os.path.isfile(backend):
                    os.unlink(backend)
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

