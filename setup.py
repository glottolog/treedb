# setup.py

import io
from setuptools import setup, find_packages

setup(
    name='treedb',
    version='0.2.dev0',
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
    python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*',
    install_requires=[
        'configparser; python_version < "3"',
        'pathlib2; python_version < "3.5"',
        'scandir; python_version < "3.5"',
        'sqlalchemy>=1.0.14',
    ],
    extras_require={
        'dev': ['flake8', 'pep8-naming', 'wheel', 'twine'],
    },
    long_description=io.open('README.rst', encoding='utf-8').read(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
