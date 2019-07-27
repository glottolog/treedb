# _compat.py - Python 2/3 compatibility

import io
import sys
import operator

PY2 = (sys.version_info.major == 2)


if PY2:
    import pathlib2 as pathlib

    from scandir import scandir

    from inspect import getargspec as getfullargspec

    iteritems = operator.methodcaller('iteritems')

    make_csv_io = io.BytesIO

    def get_csv_io_bytes(b, encoding):
        return b

    def csv_open(filename, mode, encoding):
        if not mode.endswith('b'):
            mode = mode + 'b'
        return io.open(filename, mode)

    def csv_write(writer, encoding, header, rows):
        writer.writerow([h.encode(encoding) for h in header])
        for r in rows:
            writer.writerow([unicode(col).encode(encoding) if col else col
                             for col in r])


else:
    import pathlib

    from os import scandir

    from inspect import getfullargspec

    def iteritems(d):
        return iter(d.items())

    make_csv_io = io.StringIO

    def get_csv_io_bytes(s, encoding):
        return s.encode(encoding)

    def csv_open(filename, mode, encoding):
        return io.open(filename, mode, newline='', encoding=encoding)

    def csv_write(writer, encoding, header, rows):
        writer.writerow(header)
        writer.writerows(rows)
