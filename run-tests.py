#!/usr/bin/env python3
# flake8: noqa

"""Run the tests with https://pytest.org."""

import pathlib
import platform
import sys

import pytest

SELF = pathlib.Path(__file__)

ARGS = [#'--run-writes',
        #'--skip-slow',
        #'--skip-pandas',
        #'--skip-sqlparse',
        #'-k', 'not reads',
        #'-k', 'indent',
        #'--collect-only',
        #'--capture', 'no',  # a.k.a '-s'
        #'--verbose',
        #'--showlocals',  # a.k.a. '-l'
        #'--pdb',
        #'--exitfirst',  # a.k.a. '-x'
        #'-W', 'error',
        #'--loglevel-debug',
        #'--log-sql',
        #'--no-sqlalchemy-warn-20',
        #'--file-engine',
        #'--glottolog-tag', 'master',
        #'--glottolog-tag', 'v5.2.1',
        #'--glottolog-tag', 'v5.2',
        #'--glottolog-tag', 'v5.1',
        #'--glottolog-tag', 'v5.0',
        #'--glottolog-tag', 'v4.8-treedb-fixes',
        #'--glottolog-tag', 'v4.8',
        #'--glottolog-tag', 'v4.7',
        #'--glottolog-tag', 'v4.6',
        #'--glottolog-tag', 'v4.5',
        #'--glottolog-tag', 'v4.4',
        #'--glottolog-tag', 'v4.3-treedb-fixes',
        #'--glottolog-tag', 'v4.2.1',
        #'--glottolog-tag', 'v4.1',
        #'--glottolog-repo-root', './glottolog/',
        #'--rebuild', '--force-rebuild',
        #'--exclude-raw',
       ]

if 'idlelib' in sys.modules:
    ARGS += ['--capture', 'sys', '--color', 'no']
    if platform.system() == 'Windows':
        ARGS.append('--pdb')


print('run', [SELF.name] + sys.argv[1:])

if '--installed' in sys.argv[1:]:
    sys.argv[1:] = [a for a in sys.argv[1:] if a != '--installed']
    path_item = sys.path.pop(0)
    print(f'removed {path_item!r} from sys.path')
    ARGS += ['--import-mode', 'append']

args = ARGS + sys.argv[1:]

print(f'pytest.main({args!r})')
sys.exit(pytest.main(args))
