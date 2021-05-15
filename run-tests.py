#!/usr/bin/env python3

import platform
import sys

import pytest

ARGS = [#'--run-writes',
        #'--skip-slow',
        #'--skip-pandas',
        #'--skip-sqlparse',
        #'-k', 'not reads',
        #'--collect-only',
        '--capture=no',  # a.k.a '-s'
        #'--verbose',
        #'--showlocals',  # a.k.a. '-l'
        #'--pdb',
        #'--exitfirst',  # a.k.a. '-x'
        #'--loglevel-debug',
        #'--log-sql',
        #'-W', 'error',
        #'--file-engine',
        #'--glottolog-tag', 'master',
        #'--glottolog-tag', 'v4.4',
        #'--glottolog-tag', 'v4.3-treedb-fixes',
        #'--glottolog-tag', 'v4.2.1',
        #'--glottolog-tag', 'v4.1',
        #'--glottolog-repo-root', './glottolog/',
        #'--rebuild', '--force-rebuild',
        #'--exclude-raw',
        ]

if 'idlelib' in sys.modules:
    ARGS += ['--capture=sys', '--color=no']

if platform.system() == 'Windows':
    ARGS.append('--pdb')

if '--installed' in sys.argv[1:]:
    sys.argv[1:] = [a for a in sys.argv[1:] if a != '--installed']
    sys.path.pop(0)

args = sys.argv[1:] + ARGS

print(f'pytest.main({args!r})')
sys.exit(pytest.main(args))
