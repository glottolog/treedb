#!/usr/bin/env python3

import platform
import sys

import pytest

ARGS = [
    '-s',
    #'--exitfirst',
    #'--file-engine',
    #'--glottolog-tag', 'master',
    #'--exclude-raw',
    #'--rebuild',
    #'--force-rebuild',
]

if 'idlelib' in sys.modules:
    ARGS.extend(['--capture=sys', '--color=no'])

if platform.system() == 'Windows':
    ARGS.append('--pdb')

sys.exit(pytest.main(ARGS + sys.argv[1:]))
