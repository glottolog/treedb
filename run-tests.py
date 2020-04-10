#!/usr/bin/env python3

import platform
import sys

import pytest

ARGS = [
    '-s',
    #'--exitfirst',
    #'--pdb',
]

if 'idlelib' in sys.modules:
    ARGS.extend(['--capture=sys', '--color=no'])

status = pytest.main(ARGS + sys.argv[1:])

if platform.system() == 'Windows':
    input('enter any string to exit: ')

sys.exit(status)
