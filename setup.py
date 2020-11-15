# setup.py

import pathlib
from setuptools import setup, find_packages

setup(
    name='treedb',
    version='1.3.6.dev0',
    author='Sebastian Bank',
    author_email='sebastian.bank@uni-leipzig.de',
    description='Glottolog languoid tree as SQLite database',
    keywords='glottolog languoids sqlite3 database',
    license='MIT',
    url='https://github.com/glottolog/treedb',
    project_urls={
        'Changelog': 'https://github.com/glottolog/treedb/blob/master/CHANGES.txt',
        'Issue Tracker': 'https://github.com/glottolog/treedb/issues',
    },
    packages=find_packages(),
    platforms='any',
    python_requires='>=3.6',
    install_requires=[
        'csv23~=0.3',
        'sqlalchemy~=1.3',
    ],
    extras_require={
        'dev': ['tox>=3', 'flake8', 'pep8-naming', 'wheel', 'twine'],
        'test': ['pytest>=5.2', 'pytest-cov'],
        'pretty': ['sqlparse>=0.3'],
        'pandas': ['pandas'],
    },
    long_description=pathlib.Path('README.rst').read_text(encoding='utf-8'),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
