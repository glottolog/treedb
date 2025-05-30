import pathlib
from setuptools import setup, find_packages

setup(
    name='treedb',
    version='2.6.4',
    author='Sebastian Bank',
    author_email='sebastian.bank@uni-leipzig.de',
    description='Glottolog languoid tree as SQLite database',
    keywords='glottolog languoids sqlite3 database',
    license='MIT',
    url='https://github.com/glottolog/treedb',
    project_urls={
        'Changelog': 'https://github.com/glottolog/treedb/blob/master/CHANGES.rst',
        'Issue Tracker': 'https://github.com/glottolog/treedb/issues',
        'CI': 'https://github.com/glottolog/treedb/actions',
        'Coverage': 'https://codecov.io/gh/glottolog/treedb',
    },
    packages=find_packages(),
    platforms='any',
    python_requires='>=3.9',
    install_requires=[
        'csv23~=0.3',
        'pycountry==24.6.1',  # date-based versioning scheme
        'sqlalchemy>=1.4.24',
    ],
    extras_require={
        'dev': ['tox>=3', 'flake8', 'pep8-naming', 'wheel', 'twine'],
        'test': ['pytest>=6', 'pytest-cov', 'coverage'],
        'pretty': ['sqlparse>=0.3'],
        'pandas': ['pandas>=1'],
    },
    long_description=pathlib.Path('README.rst').read_text(encoding='utf-8'),
    long_description_content_type='text/x-rst',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ],
)
