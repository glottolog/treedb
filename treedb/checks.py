# checks.py

from __future__ import unicode_literals
from __future__ import print_function

import itertools

from ._compat import getfullargspec

import sqlalchemy as sa
import sqlalchemy.orm

from .backend import Session, Dataset

from .models import (FAMILY, LANGUAGE, DIALECT,
                     SPECIAL_FAMILIES, BOOKKEEPING,
                     Languoid, Altname)

__all__ = ['check']


def check(func=None):
    """Run consistency/sanity checks on database."""
    if func is not None:
        try:
            check.registered.append(func)
        except AttributeError:
            check.registered = [func]
        return func

    passed = True
    for func in check.registered:
        session = Session()
        ns = {'invalid_query': staticmethod(func), '__doc__': func.__doc__}
        check_cls = type(str('%sCheck' % func.__name__), (Check,), ns)
        check_inst = check_cls(session)

        try:
            check_passed = check_inst.validate()
        finally:
            session.close()

        if not check_passed:
            passed = False

    return passed


class Check(object):

    detail = True

    def __init__(self, session):
        self.session = session
        self.query = self.invalid_query(session)

    def invalid_query(self, session):
        raise NotImplementedError

    def validate(self):
        self.invalid_count = self.query.count()
        print(self)

        if self.invalid_count:
            if self.detail:
                self.invalid = self.query.all()
                self.show_detail(self.invalid, self.invalid_count)
            return False
        else:
            self.invalid = []
            return True

    def __str__(self):
        if self.invalid_count:
            msg = '%d invalid\n    (violating %s)' % (self.invalid_count, self.__doc__)
        else:
            msg = 'OK'
        return '%s: %s' % (self.__class__.__name__, msg)

    @staticmethod
    def show_detail(invalid, invalid_count, number=25):
        ids = (i.id for i in itertools.islice(invalid, number))
        cont = ', ...' if number < invalid_count else ''
        print('    %s%s' % (', '.join(ids), cont))


def docformat(func):
    spec = getfullargspec(func)
    defaults = dict(zip(spec.args[-len(spec.defaults):], spec.defaults))
    func.__doc__ = func.__doc__ % defaults
    return func


@check
@docformat
def valid_glottocode(session, pattern=r'^[a-z0-9]{4}\d{4}$'):
    """Glottocodes match %(pattern)r."""
    return session.query(Languoid).order_by('id')\
        .filter(~Languoid.id.op('REGEXP')(pattern))


@check
@docformat
def valid_iso639_3(session, pattern=r'^[a-z]{3}$'):
    """Iso codes match %(pattern)r."""
    return session.query(Languoid).order_by('id')\
        .filter(~Languoid.iso639_3.op('REGEXP')(pattern))


@check
@docformat
def valid_hid(session, pattern=r'^(?:[a-z]{3}|NOCODE_[A-Z][a-zA-Z0-9-]+)$'):
    """Hids match %(pattern)r."""
    return session.query(Languoid).order_by('id')\
        .filter(~Languoid.hid.op('REGEXP')(pattern))


@check
def clean_name(session):
    """Glottolog names lack problematic characters."""

    def cond(col):
        yield col.startswith(' ')
        yield col.endswith(' ')
        yield col.op('REGEXP')('[`_*:\xa4\xab\xb6\xbc]')  # \xa4.. common in mojibake

    return session.query(Languoid).order_by('id')\
        .filter(sa.or_(
            Languoid.altnames.any(sa.or_(*cond(Altname.name)), provider='glottolog'),
            *cond(Languoid.name)))


@check
def family_parent(session):
    """Parent of a family is a family."""
    return session.query(Languoid).filter_by(level=FAMILY).order_by('id')\
        .join(Languoid.parent, aliased=True)\
        .filter(Languoid.level != FAMILY)


@check
def language_parent(session):
    """Parent of a language is a family."""
    return session.query(Languoid).filter_by(level=LANGUAGE).order_by('id')\
        .join(Languoid.parent, aliased=True)\
        .filter(Languoid.level != FAMILY)


@check
def dialect_parent(session):
    """Parent of a dialect is a language or dialect."""
    return session.query(Languoid).filter_by(level=DIALECT).order_by('id')\
        .join(Languoid.parent, aliased=True)\
        .filter(Languoid.level.notin_([LANGUAGE, DIALECT]))


@check
def family_children(session):
    """Family has at least one subfamily or language."""
    return session.query(Languoid).filter_by(level=FAMILY).order_by('id')\
        .filter(~Languoid.children.any(
            Languoid.level.in_([FAMILY, LANGUAGE])))


@check
def family_languages(session):
    """Family has at least two languages (except 'Unclassified ...')."""
    family, child = (sa.orm.aliased(Languoid) for _ in range(2))
    tree = Languoid.tree(include_self=True, with_terminal=True)
    return session.query(Languoid).filter_by(level=FAMILY).order_by('id')\
        .filter(~Languoid.name.startswith('Unclassified '))\
        .filter(~session.query(family).filter_by(level=FAMILY)
            .filter(family.name.in_(SPECIAL_FAMILIES))
            .join(tree, tree.c.parent_id == family.id)
            .filter_by(terminal=True, child_id=Languoid.id)
            .exists())\
        .filter(session.query(sa.func.count())
            .select_from(child).filter_by(level=LANGUAGE)
            .join(tree, tree.c.child_id == child.id)
            .filter_by(parent_id=Languoid.id).as_scalar() < 2)


@check
def bookkeeping_no_children(session):
    """Bookkeeping languoids lack children (book1242 is flat)."""
    return session.query(Languoid).order_by('id')\
        .filter(Languoid.parent.has(name=BOOKKEEPING))\
        .filter(Languoid.children.any())


@check
def no_empty_files(session):
    exclude_raw = session.query(Dataset.exclude_raw).scalar()
    if exclude_raw:
        return session.query(sa.true()).filter(sa.false())

    from .raw import File, Value
    return session.query(File)\
           .filter(~sa.exists().where(Value.file_id == File.id))
