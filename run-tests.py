#!/usr/bin/env python3
# flake8: noqa

"""Run the tests with https://pytest.org."""

import pathlib
import platform
import sys

import pytest

ARGS = [#'--glottolog-repo-root', './glottolog/',
        #'--glottolog-tag', 'master',
        #'--glottolog-tag', 'v5.3',
        #'--glottolog-tag', 'v5.2.1',
        #'--glottolog-tag', 'v5.2',
        #'--glottolog-tag', 'v5.1',
        #'--glottolog-tag', 'v5.0',
        #'--file-engine',
        #'--rebuild', '--force-rebuild',
        #'--exclude-raw',
        #'--run-writes',
        #'--skip-slow',
        #'--skip-pandas',
        #'--skip-sqlparse',
        #'-k', 'not reads',
        #'-k', 'indent',
        #'--collect-only',
        #'--capture', 'no',  # a.k.a -s
        #'--verbose',
        #'--showlocals',  # a.k.a. -l
        #'--pdb',
        #'--exitfirst',  # a.k.a. -x
        #'-W', 'error',
        #'--loglevel-debug',
        #'--log-sql',
       ]

if platform.system() == 'Windows' and 'idlelib' in sys.modules:
    ARGS += ['-p', 'no:faulthandler']

if platform.system() == 'Windows':
    ARGS.append('--pdb')


print('run', [pathlib.Path(__file__).name] + sys.argv[1:])

if '--installed' in sys.argv[1:]:
    sys.argv[1:] = [a for a in sys.argv[1:] if a != '--installed']
    path_item = sys.path.pop(0)
    print(f'removed {path_item!r} from sys.path')
    ARGS += ['--import-mode', 'append']

args = ARGS + sys.argv[1:]

# https://docs.pytest.org/en/stable/reference/reference.html#pytest-main
print(f'pytest.main({args!r})')
if (returncode := pytest.main(args)):
    print('FAILED:', returncode)
    sys.exit(returncode)
print('PASSED.')
