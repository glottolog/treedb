# checks.py - check application-specific invariants

import inspect
import itertools
import logging

import pytest
import sqlalchemy as sa
import sqlalchemy.orm

from . import (ENGINE, ROOT,
               Session)

from .backend.models import Dataset

from .models import (FAMILY, LANGUAGE, DIALECT,
                     SPECIAL_FAMILIES, BOOKKEEPING,
                     Languoid, Altname, AltnameProvider)

__all__ = ['check',
           'compare_with_files']


log = logging.getLogger(__name__)


def check(func=None, *, bind=ENGINE):
    """Run consistency/sanity checks on database."""
    if func is not None:
        try:
            check.registered.append(func)
        except AttributeError:
            check.registered = [func]
        return func

    with bind.connect() as conn:
        exclude_raw = conn.scalar(sa.select(Dataset.exclude_raw))

    passed = True

    for func in check.registered:
        with Session(bind=bind) as session:
            ns = {'invalid_query': staticmethod(func), '__doc__': func.__doc__}
            check_cls = type(f'{func.__name__}Check', (Check,), ns)

            kwargs = ({'exclude_raw': exclude_raw}
                      if func.__name__ == 'no_empty_files' else {})

            check_inst = check_cls(session, **kwargs)

            log.debug('validate %r', func.__name__)
            check_passed = check_inst.validate()

            if not check_passed:
                passed = False

    return passed


class Check(object):

    detail = True

    def __init__(self, session, **kwargs):
        self.session = session
        self.query = self.invalid_query(**kwargs)

    def invalid_query(self, **kwargs):  # pragma: no cover
        raise NotImplementedError

    def validate(self):
        query = self.query\
                .with_only_columns(sa.func.count().label('invalid_count'))\
                .order_by(None)

        with self.session as session:
            self.invalid_count = session.execute(query).scalar()

        log.debug('invalid count: %d', self.invalid_count)
        print(self)

        if self.invalid_count:
            if self.detail:
                self.invalid = session.execute(self.query).all()
                self.show_detail(self.invalid, self.invalid_count)
            return False
        else:
            self.invalid = []
            return True

    def __str__(self):
        if self.invalid_count:
            msg = (f'{self.invalid_count:d} invalid\n'
                   f'    (violating {self.__doc__})')
        else:
            msg = 'OK'
        return f'{self.__class__.__name__}: {msg}'

    @staticmethod
    def show_detail(invalid, invalid_count, number=25):
        ids = (i.id for i in itertools.islice(invalid, number))
        cont = ', ...' if number < invalid_count else ''
        print(f"    {', '.join(ids)}{cont}")


def docformat(func):
    sig = inspect.signature(func)
    defaults = {n: p.default for n, p in sig.parameters.items()
                if p.default != inspect.Parameter.empty}
    func.__doc__ = func.__doc__.format_map(defaults)
    return func


@check
@docformat
def valid_glottocode(*, pattern=r'^[a-z0-9]{4}\d{4}$'):
    """Glottocodes match {pattern!r}."""
    return sa.select(Languoid)\
           .order_by('id')\
           .where(~Languoid.id.regexp_match(pattern))


@check
@docformat
def valid_iso639_3(*, pattern=r'^[a-z]{3}$'):
    """Iso codes match {pattern!r}."""
    return sa.select(Languoid)\
           .order_by('id')\
           .where(~Languoid.iso639_3.regexp_match(pattern))


@check
@docformat
def valid_hid(*, pattern=r'^(?:[a-z]{3}|NOCODE_[A-Z][a-zA-Z0-9-]+)$'):
    """Hids match {pattern!r}."""
    return sa.select(Languoid)\
           .order_by('id')\
           .where(~Languoid.hid.regexp_match(pattern))


@check
def clean_name():
    """Glottolog names lack problematic characters."""
    gl = sa.select(AltnameProvider.id)\
         .filter_by(name='glottolog')\
         .scalar_subquery()

    def cond(col):
        yield col.startswith(' ')
        yield col.endswith(' ')
        yield col.regexp_match('[`_*:\xa4\xab\xb6\xbc]')  # \xa4.. common in mojibake

    match_gl = Languoid.altnames\
               .any(sa.or_(*cond(Altname.name)), provider_id=gl)

    return sa.select(Languoid)\
           .order_by('id')\
           .where(sa.or_(match_gl, *cond(Languoid.name)))


@check
def family_parent():
    """Parent of a family is a family."""
    parent = sa.orm.aliased(Languoid)
    return sa.select(Languoid)\
           .filter_by(level=FAMILY)\
           .order_by('id')\
           .join(Languoid.parent.of_type(parent))\
           .where(parent.level != FAMILY)


@check
def language_parent():
    """Parent of a language is a family."""
    parent = sa.orm.aliased(Languoid)
    return sa.select(Languoid)\
           .filter_by(level=LANGUAGE)\
           .order_by('id')\
           .join(Languoid.parent.of_type(parent))\
           .where(parent.level != FAMILY)


@check
def dialect_parent():
    """Parent of a dialect is a language or dialect."""
    parent = sa.orm.aliased(Languoid)
    return sa.select(Languoid)\
           .filter_by(level=DIALECT)\
           .order_by('id')\
           .join(Languoid.parent.of_type(parent))\
           .where(parent.level.notin_([LANGUAGE, DIALECT]))


@check
def family_children():
    """Family has at least one subfamily or language."""
    return sa.select(Languoid)\
           .filter_by(level=FAMILY)\
           .order_by('id')\
           .where(~Languoid.children.any(Languoid.level.in_([FAMILY,
                                                             LANGUAGE])))


@check
def family_languages():
    """Family has at least two languages (except 'Unclassified ...')."""
    Family, Child = (sa.orm.aliased(Languoid, name=n) for n in ('family', 'child'))

    tree = Languoid.tree(include_self=False, with_terminal=True)

    return sa.select(Languoid)\
           .filter_by(level=FAMILY)\
           .order_by('id')\
           .where(~Languoid.name.startswith('Unclassified '))\
           .where(~sa.select(Family)
                  .filter_by(level=FAMILY)
                  .where(Family.name.in_(SPECIAL_FAMILIES))
                  .join(tree, Family.id == tree.c.parent_id)
                  .filter_by(child_id=Languoid.id, terminal=True)
                  .exists())\
           .where(sa.select(sa.func.count())
                  .select_from(Child)
                  .filter_by(level=LANGUAGE)
                  .join(tree, Child.id == tree.c.child_id)
                  .filter_by(parent_id=Languoid.id)
                  .scalar_subquery() < 2)


@check
def bookkeeping_no_children():
    """Bookkeeping languoids lack children (book1242 is flat)."""
    return sa.select(Languoid)\
           .select_from(Languoid)\
           .order_by('id')\
           .where(Languoid.parent.has(name=BOOKKEEPING))\
           .where(Languoid.children.any())


@check
def no_empty_files(*, exclude_raw):
    if exclude_raw:  # pragma: no cover
        pytest.skip('skipped from exclude_raw=True')
        return sa.select(sa.true()).where(sa.false())

    from .raw import File, Value

    return sa.select(File)\
           .select_from(File)\
           .where(~sa.exists().where(Value.file_id == File.id))


def compare_with_files(bind=ENGINE, *, from_raw=True, root=ROOT):
    from . import languoids

    def compare(left, right):
        same = True
        for l, r in itertools.zip_longest(left, right):
            if l != r:
                same = False
                print('', '', l, '', r, '', '', sep='\n')

        return same

    languoids_from_files = languoids.iterlanguoids(root,
                                                   ordered=True)

    languoids_from_bind = languoids.iterlanguoids(bind,
                                                  from_raw=from_raw,
                                                  ordered='path')

    return compare(languoids_from_files, languoids_from_bind)
