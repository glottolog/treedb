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
    #'--glottolog-tag', 'v4.2',
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

if '--installed' in sys.argv[1:]:
    sys.argv[1:] = [a for a in sys.argv[1:] if a != '--installed']
    sys.path.pop(0)

sys.exit(pytest.main(ARGS + sys.argv[1:]))
