# setup.py

import pathlib
from setuptools import setup, find_packages

setup(
    name='treedb',
    version='0.4.dev0',
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
        'csv23~=0.2',
        'sqlalchemy>=1.0.14',
    ],
    extras_require={
        'dev': ['flake8', 'pep8-naming', 'wheel', 'twine'],
    },
    long_description=pathlib.Path('README.rst').read_text(encoding='utf-8'),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
