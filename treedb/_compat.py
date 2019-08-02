# _compat.py - Python 2/3 compatibility

import io
import sys
import operator

ENCODING = 'utf-8'

PY2 = (sys.version_info.major == 2)


if PY2:
    iteritems = operator.methodcaller('iteritems')

    from itertools import (imap as map,
                           izip as zip,
                           izip_longest as zip_longest)

    import pathlib2 as pathlib

    from scandir import scandir

    from inspect import getargspec as getfullargspec

    make_csv_io = io.BytesIO

    def get_csv_io_bytes(b, encoding=ENCODING):
        return b

    def csv_open(filename, mode, encoding=ENCODING):
        if not mode.endswith('b'):
            mode = mode + 'b'
        return io.open(filename, mode)

    def csv_write(writer, rows, header=None, encoding=ENCODING):
        if header is not None:
            writer.writerow([h.encode(encoding) for h in header])
        for r in rows:
            writer.writerow([unicode(col).encode(encoding) if col else col
                             for col in r])


else:
    def iteritems(d):
        return iter(d.items())

    map = map
    zip = zip
    from itertools import zip_longest

    import pathlib

    from os import scandir

    from inspect import getfullargspec

    make_csv_io = io.StringIO

    def get_csv_io_bytes(s, encoding=ENCODING):
        return s.encode(encoding)

    def csv_open(filename, mode, encoding=ENCODING):
        return io.open(filename, mode, newline='', encoding=encoding)

    def csv_write(writer, rows, header=None, encoding=ENCODING):
        if header is not None:
            writer.writerow(header)
        writer.writerows(rows)
