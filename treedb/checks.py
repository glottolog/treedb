# checks.py - check application-specific invariants

import inspect
import itertools
import logging

import sqlalchemy as sa
import sqlalchemy.orm

from .backend import Session, Dataset

from .models import (FAMILY, LANGUAGE, DIALECT,
                     SPECIAL_FAMILIES, BOOKKEEPING,
                     Languoid, Altname, AltnameProvider)

from . import ENGINE

__all__ = ['check']


log = logging.getLogger(__name__)


def check(func=None, *, bind=ENGINE):
    """Run consistency/sanity checks on database."""
    if func is not None:
        try:
            check.registered.append(func)
        except AttributeError:
            check.registered = [func]
        return func

    passed = True
    with bind.connect() as conn:
        for func in check.registered:
            ns = {'invalid_query': staticmethod(func), '__doc__': func.__doc__}
            check_cls = type(f'{func.__name__}Check', (Check,), ns)

            session = Session(bind=conn)

            check_inst = check_cls(session)

            try:
                log.debug('validate %r', func.__name__)
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

    def invalid_query(self, session):  # pragma: no cover
        raise NotImplementedError

    def validate(self):
        self.invalid_count = self.query.count()
        log.debug('invalid count: %d', self.invalid_count)
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
def valid_glottocode(session, *, pattern=r'^[a-z0-9]{4}\d{4}$'):
    """Glottocodes match {pattern!r}."""
    return session.query(Languoid).order_by('id')\
           .filter(~Languoid.id.op('REGEXP')(pattern))


@check
@docformat
def valid_iso639_3(session, *, pattern=r'^[a-z]{3}$'):
    """Iso codes match {pattern!r}."""
    return session.query(Languoid).order_by('id')\
           .filter(~Languoid.iso639_3.op('REGEXP')(pattern))


@check
@docformat
def valid_hid(session, *, pattern=r'^(?:[a-z]{3}|NOCODE_[A-Z][a-zA-Z0-9-]+)$'):
    """Hids match {pattern!r}."""
    return session.query(Languoid).order_by('id')\
           .filter(~Languoid.hid.op('REGEXP')(pattern))


@check
def clean_name(session):
    """Glottolog names lack problematic characters."""
    gl = session.query(AltnameProvider.id)\
         .filter_by(name='glottolog')\
         .as_scalar()

    def cond(col):
        yield col.startswith(' ')
        yield col.endswith(' ')
        yield col.op('REGEXP')('[`_*:\xa4\xab\xb6\xbc]')  # \xa4.. common in mojibake

    match_gl = Languoid.altnames\
               .any(sa.or_(*cond(Altname.name)), provider_id=gl)

    return session.query(Languoid).order_by('id')\
           .filter(sa.or_(match_gl, *cond(Languoid.name)))


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
           .filter(~Languoid.children.any(Languoid.level.in_([FAMILY,
                                                              LANGUAGE])))


@check
def family_languages(session):
    """Family has at least two languages (except 'Unclassified ...')."""
    Family, Child = (sa.orm.aliased(Languoid, name=n) for n in ('family', 'child'))

    tree = Languoid.tree(include_self=False, with_terminal=True)

    return session.query(Languoid).filter_by(level=FAMILY).order_by('id')\
           .filter(~Languoid.name.startswith('Unclassified '))\
           .filter(~session.query(Family).filter_by(level=FAMILY)
                   .filter(Family.name.in_(SPECIAL_FAMILIES))
                   .join(tree, Family.id == tree.c.parent_id)
                   .filter_by(child_id=Languoid.id, terminal=True)
                   .exists())\
           .filter(session.query(sa.func.count())
                   .select_from(Child).filter_by(level=LANGUAGE)
                   .join(tree, Child.id == tree.c.child_id)
                   .filter_by(parent_id=Languoid.id)
                   .as_scalar() < 2)


@check
def bookkeeping_no_children(session):
    """Bookkeeping languoids lack children (book1242 is flat)."""
    return session.query(Languoid).order_by('id')\
           .filter(Languoid.parent.has(name=BOOKKEEPING))\
           .filter(Languoid.children.any())


@check
def no_empty_files(session):
    exclude_raw = session.query(Dataset.exclude_raw).scalar()
    if exclude_raw:  # pragma: no cover
        return session.query(sa.true()).filter(sa.false())

    from .raw import File, Value

    return session.query(File)\
           .filter(~sa.exists().where(Value.file_id == File.id))
