#!/usr/bin/env python3

import platform
import sys

import pytest

ARGS = [
    '-s',
    #'--exitfirst',
    #'--file-engine',
    #'--glottolog-tag', 'master',
    #'--glottolog-tag', 'v4.1',
    #'--glottolog-repo-root', './glottolog/',
    #'--rebuild',
    #'--force-rebuild',
    #'--exclude-raw',
    #'--loglevel-debug',
]

if 'idlelib' in sys.modules:
    ARGS += ['--capture=sys', '--color=no']

if platform.system() == 'Windows':
    ARGS.append('--pdb')

sys.exit(pytest.main(ARGS + sys.argv[1:]))
