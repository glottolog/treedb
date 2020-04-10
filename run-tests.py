#!/usr/bin/env python3

import sys

import pytest

ARGS = [
    #'--exitfirst',
    #'--pdb',
]

if 'idlelib' in sys.modules:
    ARGS.extend(['--capture=sys', '--color=no'])

status = pytest.main(ARGS + sys.argv[1:])

input('enter any string to exit: ')
sys.exit(status)
