# models.py - sqlalchemy schema, loading, checking, and queries for sqlite3 db

from __future__ import unicode_literals

from ._compat import iteritems

import sqlalchemy as sa
import sqlalchemy.orm

from . import languoids as _languoids
from . import backend as _backend

__all__ = ['Languoid', 'load']

LEVEL = ('family', 'language', 'dialect')

FAMILY, LANGUAGE, DIALECT = LEVEL

BOOKKEEPING = 'Bookkeeping'

SPECIAL_FAMILIES = (
    'Unattested',
    'Unclassifiable',
    'Pidgin',
    'Mixed Language',
    'Artificial Language',
    'Speech Register',
    'Sign Language',
)

MACROAREA = {
    'North America', 'South America',
    'Eurasia',
    'Africa',
    'Australia', 'Papunesia',
}

LINK_SCHEME = {'https', 'http'}

SOURCE_PROVIDER = {'glottolog'}

ALTNAME_PROVIDER = {
    'multitree', 'lexvo', 'hhbib_lgcode',
    'wals', 'wals other', 'moseley & asher (1994)', 'ruhlen (1987)',
    'glottolog', 'ethnologue', 'elcat',
}

TRIGGER_FIELD = {'lgcode', 'inlg'}

IDENTIFIER_SITE = {
    'multitree', 'endangeredlanguages',
    'wals', 'languagelandscape',
}

CLASSIFICATION = {
    'sub': (False, 'sub'), 'subrefs': (True, 'sub'),
    'family': (False, 'family'), 'familyrefs': (True, 'family')
}

CLASSIFICATION_KIND = {c for _, c in CLASSIFICATION.values()}

ENDANGERMENT_STATUS = (
    'not endangered',
    'threatened', 'shifting',
    'moribund', 'nearly extinct',
    'extinct',
)

ENDANGERMENT_SOURCE = {'E20', 'E21', 'E22', 'ElCat', 'UNESCO', 'Glottolog'}

EL_COMMENT_TYPE = {'Missing', 'Spurious'}

ISORETIREMENT_REASON = {'split', 'merge', 'duplicate', 'non-existent', 'change'}


class Languoid(_backend.Model):

    __tablename__ = 'languoid'

    id = sa.Column(sa.String(8), sa.CheckConstraint('length(id) = 8'), primary_key=True)
    level = sa.Column(sa.Enum(*LEVEL), nullable=False)
    name = sa.Column(sa.String, sa.CheckConstraint("name != ''"), nullable=False, unique=True)
    parent_id = sa.Column(sa.ForeignKey('languoid.id'), index=True)
    hid = sa.Column(sa.Text, sa.CheckConstraint('length(hid) >= 3'), unique=True)
    iso639_3 = sa.Column(sa.String(3), sa.CheckConstraint('length(iso639_3) = 3'), unique=True)
    latitude = sa.Column(sa.Float, sa.CheckConstraint('latitude BETWEEN -90 AND 90'))
    longitude = sa.Column(sa.Float, sa.CheckConstraint('longitude BETWEEN -180 AND 180'))

    __table_args__ = (
        sa.CheckConstraint('(latitude IS NULL) = (longitude IS NULL)'),
    )

    def __repr__(self):
        hid_iso = ['%s=%r' % (n, getattr(self, n)) for n in ('hid', 'iso639_3') if getattr(self, n)]
        return '<%s id=%r level=%r name=%r%s>' % (self.__class__.__name__,
            self.id, self.level, self.name, ' ' + ' '.join(hid_iso) if hid_iso else '')

    parent = sa.orm.relationship('Languoid', remote_side=[id])
    children = sa.orm.relationship('Languoid', remote_side=[parent_id], order_by=id)

    macroareas = sa.orm.relationship('Macroarea', secondary='languoid_macroarea', order_by='Macroarea.name',
                                      back_populates='languoids')
    countries = sa.orm.relationship('Country', secondary='languoid_country', order_by='Country.id',
                                    back_populates='languoids')

    links = sa.orm.relationship('Link', back_populates='languoid', order_by='Source.ord')
    sources = sa.orm.relationship('Source', back_populates='languoid', order_by='[Source.provider, Source.ord]')
    altnames = sa.orm.relationship('Altname', back_populates='languoid', order_by='[Altname.provider, Altname.ord]')
    triggers = sa.orm.relationship('Trigger', back_populates='languoid', order_by='[Trigger.field, Trigger.ord]')
    identifiers = sa.orm.relationship('Identifier', back_populates='languoid', order_by='Identifier.site')

    subclassificationcomment = sa.orm.relationship('ClassificationComment', uselist=False,
        primaryjoin="and_(ClassificationComment.languoid_id == Languoid.id, ClassificationComment.kind == 'sub')")
    subclassificationrefs = sa.orm.relationship('ClassificationRef', order_by='ClassificationRef.ord',
        primaryjoin="and_(ClassificationRef.languoid_id == Languoid.id, ClassificationRef.kind == 'sub')")
    familyclassificationcomment = sa.orm.relationship('ClassificationComment', uselist=False,
        primaryjoin="and_(ClassificationComment.languoid_id == Languoid.id, ClassificationComment.kind == 'family')")
    familyclassificationrefs = sa.orm.relationship('ClassificationRef', order_by='ClassificationRef.ord',
        primaryjoin="and_(ClassificationRef.languoid_id == Languoid.id, ClassificationRef.kind == 'family')")

    endangerment = sa.orm.relationship('Endangerment', uselist=False, back_populates='languoid')
    ethnologue_comment = sa.orm.relationship('EthnologueComment', uselist=False, back_populates='languoid')
    iso_retirement = sa.orm.relationship('IsoRetirement', uselist=False, back_populates='languoid')

    @classmethod
    def tree(cls, include_self=False, with_steps=False, with_terminal=False):
        child, parent = (sa.orm.aliased(cls, name=n) for n in ('child', 'parent'))
        tree_1 = sa.select([child.id.label('child_id')])
        if include_self:
            parent_id = child.id
        else:
            parent_id = child.parent_id
            tree_1.append_whereclause(parent_id != None)
        tree_1.append_column(parent_id.label('parent_id'))
        if with_steps:
            steps = 0 if include_self else 1
            tree_1.append_column(sa.literal(steps).label('steps'))
        if with_terminal:
            if include_self:
                terminal = sa.type_coerce(child.parent_id == None, sa.Boolean)
            else:
                terminal = sa.literal(False)
            tree_1.append_column(terminal.label('terminal'))
        tree_1 = tree_1.cte('tree', recursive=True)

        tree_2 = sa.select([tree_1.c.child_id, parent.parent_id])\
            .select_from(tree_1.join(parent, parent.id == tree_1.c.parent_id))\
            .where(parent.parent_id != None)
        if with_steps:
            tree_2.append_column((tree_1.c.steps + 1).label('steps'))
        if with_terminal:
            gparent = sa.orm.aliased(Languoid, name='grandparent')
            tree_2.append_column((gparent.parent_id == None).label('terminal'))
            tree_2 = tree_2.select_from(tree_2.froms[-1]
                .outerjoin(gparent, gparent.id == parent.parent_id))
        return tree_1.union_all(tree_2)

    @classmethod
    def path(cls, label='path', delimiter='/', include_self=True, bottomup=False, _tree=None):
        tree = _tree
        if tree is None:
            tree = cls.tree(include_self=include_self, with_steps=True, with_terminal=False)
        squery = sa.select([tree.c.parent_id.label('path_part')])\
            .where(tree.c.child_id == cls.id).correlate(cls)\
            .order_by(tree.c.steps if bottomup else tree.c.steps.desc())
        path = sa.func.group_concat(squery.c.path_part, delimiter)
        return sa.select([path]).label(label)

    @classmethod
    def path_family_language(cls, path_label='path', path_delimiter='/', include_self=True, bottomup=False,
                             family_label='family_id', language_label='language_id'):
        tree = cls.tree(include_self=include_self, with_steps=True, with_terminal=True)
        path = cls.path(label=path_label, delimiter=path_delimiter, bottomup=bottomup, _tree=tree)
        family = sa.select([tree.c.parent_id])\
            .where(tree.c.child_id == cls.id).correlate(cls)\
            .where(tree.c.steps > 0).where(tree.c.terminal == True)
        ancestor = sa.orm.aliased(Languoid)
        language = sa.select([tree.c.parent_id])\
            .where(tree.c.child_id == cls.id).correlate(cls)\
            .where(cls.level == DIALECT)\
            .where(sa.exists()
                .where(ancestor.id == tree.c.parent_id)
                .where(ancestor.level == LANGUAGE))
        return path, family.label(family_label), language.label(language_label)


class Macroarea(_backend.Model):

    __tablename__ = 'macroarea'

    name = sa.Column(sa.Enum(*sorted(MACROAREA)), primary_key=True)

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.name)

    languoids = sa.orm.relationship('Languoid', secondary='languoid_macroarea', order_by='Languoid.id',
                                    back_populates='macroareas')


languoid_macroarea = sa.Table('languoid_macroarea', _backend.Model.metadata,
    sa.Column('languoid_id', sa.ForeignKey('languoid.id'), primary_key=True),
    sa.Column('macroarea_name', sa.ForeignKey('macroarea.name'), primary_key=True))


class Country(_backend.Model):

    __tablename__ = 'country'

    id = sa.Column(sa.String(2), sa.CheckConstraint('length(id) = 2'), primary_key=True)
    name = sa.Column(sa.Text, sa.CheckConstraint("name != ''"), nullable=False, unique=True)

    def __repr__(self):
        return '<%s id=%r name=%r>' % (self.__class__.__name__, self.id, self.name)

    languoids = sa.orm.relationship('Languoid', secondary='languoid_country', order_by='Languoid.id',
                                    back_populates='countries')


languoid_country = sa.Table('languoid_country', _backend.Model.metadata,
    sa.Column('languoid_id', sa.ForeignKey('languoid.id'), primary_key=True),
    sa.Column('country_id', sa.ForeignKey('country.id'), primary_key=True))


class Link(_backend.Model):

    __tablename__ = 'link'

    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    ord = sa.Column(sa.Integer, sa.CheckConstraint('ord >= 1'), primary_key=True)
    url = sa.Column(sa.Text, sa.CheckConstraint("url != ''"), nullable=False)
    title = sa.Column(sa.Text, sa.CheckConstraint("title != ''"))
    scheme = sa.Column(sa.Text, sa.Enum(*sorted(LINK_SCHEME)))

    languoid = sa.orm.relationship('Languoid', innerjoin=True, back_populates='links')

    def __repr__(self):
        return '<%s languoid_id=%r ord=%r url=%r title=%r scheme=%r>' % (
            self.__class__.__name__,
            self.languoid_id, self.ord, self.url, self.title, self.scheme)

    @classmethod
    def printf(cls):
        return sa.case([
            (cls.title != None,
                sa.func.printf('(%s)[%s]', cls.title, cls.url)),
            ], else_=cls.url)


class Source(_backend.Model):

    __tablename__ = 'source'

    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    provider = sa.Column(sa.Text, sa.Enum(*sorted(SOURCE_PROVIDER)), primary_key=True)
    bibfile = sa.Column(sa.Text, sa.CheckConstraint("bibfile != ''"), primary_key=True)
    bibkey = sa.Column(sa.Text, sa.CheckConstraint("bibkey != ''"), primary_key=True)
    ord = sa.Column(sa.Integer, sa.CheckConstraint('ord >= 1'), nullable=False)
    pages = sa.Column(sa.Text, sa.CheckConstraint("pages != ''"))
    trigger = sa.Column(sa.Text, sa.CheckConstraint("trigger != ''"))

    __table_args__ = (
        sa.UniqueConstraint(languoid_id, provider, ord),
    )

    def __repr__(self):
        return '<%s languoid_id=%r povider=%r bibfile=%r bibkey=%r>' % (self.__class__.__name__,
            self.languoid_id, self.provider, self.bibfile, self.bibkey)

    languoid = sa.orm.relationship('Languoid', innerjoin=True, back_populates='sources')

    @classmethod
    def printf(cls):
        return sa.case([
            (sa.and_(cls.pages != None, cls.trigger != None),
                 sa.func.printf('**%s:%s**:%s<trigger "%s">', cls.bibfile, cls.bibkey, cls.pages, cls.trigger)),
            (cls.pages != None,
                 sa.func.printf('**s:%s**:%s', cls.bibfile, cls.bibkey, cls.pages)),
            (cls.trigger != None,
                 sa.func.printf('**%s:%s**<trigger "%s">', cls.bibfile, cls.bibkey, cls.trigger)),
            ], else_=sa.func.printf('**%s:%s**', cls.bibfile, cls.bibkey))


class Altname(_backend.Model):

    __tablename__ = 'altname'

    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    provider = sa.Column(sa.Text, sa.Enum(*sorted(ALTNAME_PROVIDER)), primary_key=True)
    lang = sa.Column(sa.String(3), sa.CheckConstraint("length(lang) IN (0, 2, 3) OR lang = '!'"), primary_key=True)
    name = sa.Column(sa.Text, sa.CheckConstraint("name != ''"), primary_key=True)
    ord = sa.Column(sa.Integer, sa.CheckConstraint('ord >= 1'), nullable=False)

    __table_args__ = (
        sa.UniqueConstraint(languoid_id, provider, ord),
    )

    def __repr__(self):
        return '<%s languoid_id=%r povider=%r lang=%r name=%r>' % (self.__class__.__name__,
            self.languoid_id, self.provider, self.lang, self.name)

    languoid = sa.orm.relationship('Languoid', innerjoin=True, back_populates='altnames')

    @classmethod
    def printf(cls):
        return sa.case([
            (cls.lang == '',
                 cls.name),
            (sa.between(sa.func.length(cls.lang), 2, 3),
                 sa.func.printf('%s [%s]', cls.name, cls.lang)),
            ], else_=cls.name)


class Trigger(_backend.Model):

    __tablename__ = 'trigger'

    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    field = sa.Column(sa.Enum(*sorted(TRIGGER_FIELD)), primary_key=True)
    trigger = sa.Column(sa.Text, sa.CheckConstraint("trigger != ''"), primary_key=True)
    ord = sa.Column(sa.Integer, sa.CheckConstraint('ord >= 1'), nullable=False)

    __table_args__ = (
        sa.UniqueConstraint(languoid_id, field, ord),
    )

    def __repr__(self):
        return '<%s languoid_id=%r field=%r trigger=%r>' % (self.__class__.__name__,
            self.languoid_id, self.field, self.trigger)

    languoid = sa.orm.relationship('Languoid', innerjoin=True, back_populates='triggers')


class Identifier(_backend.Model):

    __tablename__ = 'identifier'

    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    site = sa.Column(sa.Enum(*sorted(IDENTIFIER_SITE)), primary_key=True)
    identifier = sa.Column(sa.Text, sa.CheckConstraint("identifier != ''"), nullable=False)

    def __repr__(self):
        return '<%s languoid_id=%r site=%r identifier=%r>' % (self.__class__.__name__,
            self.languoid_id, self.site, self.identifier)

    languoid = sa.orm.relationship('Languoid', innerjoin=True, back_populates='identifiers')


class ClassificationComment(_backend.Model):

    __tablename__ = 'classificationcomment'

    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    kind = sa.Column(sa.Enum(*sorted(CLASSIFICATION_KIND)), primary_key=True)
    comment = sa.Column(sa.Text, sa.CheckConstraint("comment != ''"), nullable=False)

    def __repr__(self):
        return '<%s languoid_id=%r kind=%r comment=%r>' % (self.__class__.__name__,
            self.languoid_id, self.kind, self.comment)

    languoid = sa.orm.relationship('Languoid', innerjoin=True)


class ClassificationRef(_backend.Model):

    __tablename__ = 'classificationref'
    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    kind = sa.Column(sa.Enum(*sorted(CLASSIFICATION_KIND)), primary_key=True)
    bibfile = sa.Column(sa.Text, sa.CheckConstraint("bibfile != ''"), primary_key=True)
    bibkey = sa.Column(sa.Text, sa.CheckConstraint("bibkey != ''"), primary_key=True)
    ord = sa.Column(sa.Integer, sa.CheckConstraint('ord >= 1'), nullable=False)
    pages = sa.Column(sa.Text, sa.CheckConstraint("pages != ''"))

    __table_args__ = (
        sa.UniqueConstraint(languoid_id, kind, ord),
    )

    def __repr__(self):
        return '<%s languoid_id=%r kind=%r bibfile=%r bibkey=%r>' % (self.__class__.__name__,
            self.languoid_id, self.kind, self.bibfile, self.bibkey)

    languoid = sa.orm.relationship('Languoid', innerjoin=True)

    @classmethod
    def printf(cls):
        format_ = sa.case([(cls.pages != None, '**s:%s**:%s')], else_='**%s:%s**')
        return sa.func.printf(format_, cls.bibfile, cls.bibkey, cls.pages)


class Endangerment(_backend.Model):

    __tablename__ = 'endangerment'

    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    status = sa.Column(sa.Enum(*ENDANGERMENT_STATUS), nullable=False)
    source = sa.Column(sa.Enum(*sorted(ENDANGERMENT_SOURCE)), nullable=False)
    date = sa.Column(sa.DateTime, nullable=False)
    comment = sa.Column(sa.Text, sa.CheckConstraint("comment != ''"), nullable=False)

    def __repr__(self):
        return '<%s languoid_id=%r status=%r source=%r date=%r>' % (self.__class__.__name__,
            self.languoid_id, self.status, self.source, self.date)

    languoid = sa.orm.relationship('Languoid', innerjoin=True, back_populates='endangerment')


class EthnologueComment(_backend.Model):

    __tablename__ = 'ethnologuecomment'

    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    isohid = sa.Column(sa.Text, sa.CheckConstraint('length(isohid) >= 3'), nullable=False)
    comment_type = sa.Column(sa.Enum(*sorted(EL_COMMENT_TYPE)), nullable=False)
    ethnologue_versions = sa.Column(sa.Text, sa.CheckConstraint('length(ethnologue_versions) >= 3'), nullable=False)
    comment = sa.Column(sa.Text, sa.CheckConstraint("comment != ''"), nullable=False)

    def __repr__(self):
        return '<%s languoid_id=%r isohid=%r comment_type=%r ethnologue_versions=%r>' % (self.__class__.__name__,
            self.languoid_id, self.isohid, self.comment_type, self.ethnologue_versions)

    languoid = sa.orm.relationship('Languoid', innerjoin=True, back_populates='ethnologue_comment')


class IsoRetirement(_backend.Model):

    __tablename__ = 'isoretirement'

    languoid_id = sa.Column(sa.ForeignKey('languoid.id'), primary_key=True)
    code = sa.Column(sa.String(3), sa.CheckConstraint('length(code) = 3'), nullable=False)
    name = sa.Column(sa.Text, sa.CheckConstraint("name != ''"), nullable=False)
    change_request = sa.Column(sa.String(8), sa.CheckConstraint("change_request LIKE '____-___' "))
    effective = sa.Column(sa.Date, nullable=False)
    reason = sa.Column(sa.Enum(*sorted(ISORETIREMENT_REASON)), nullable=False)
    remedy = sa.Column(sa.Text, sa.CheckConstraint("remedy != ''"))
    comment = sa.Column(sa.Text, sa.CheckConstraint("comment != ''"))

    __table_args__ = (
        # TODO: fix disagreement
        sa.Index('change_request_key', sa.func.coalesce(change_request, effective)),
        sa.CheckConstraint("remedy IS NOT NULL OR reason = 'non-existent'"),
    )

    def __repr__(self):
        return '<%s languoid_id=%r code=%r name=%r change_request=%r effective=%r reason=%r remedy=%r>' % (
            self.__class__.__name__, self.languoid_id, self.code, self.name, self.change_request,
            self.effective, self.reason, self.remedy)

    languoid = sa.orm.relationship('Languoid', innerjoin=True, back_populates='iso_retirement')

    change_to = sa.orm.relationship('IsoRetirementChangeTo', order_by='IsoRetirementChangeTo.ord',
                                    back_populates='iso_retirement')


class IsoRetirementChangeTo(_backend.Model):

    __tablename__ = 'isoretirement_changeto'

    languoid_id = sa.Column(sa.ForeignKey('isoretirement.languoid_id'), primary_key=True)
    code = sa.Column(sa.String(3), sa.CheckConstraint('length(code) = 3'), primary_key=True)
    ord = sa.Column(sa.Integer, sa.CheckConstraint('ord >= 1'), nullable=False)

    __table_args__ = (
        sa.UniqueConstraint('languoid_id', 'ord'),
    )

    def __repr__(self):
        return '<%s languoid_id=%r code=%r>' % (self.__class__.__name__,
            self.languoid_id, self.code)

    iso_retirement = sa.orm.relationship('IsoRetirement', innerjoin=True, back_populates='change_to')


def load(root=None, with_raw=True, rebuild=False):
    """Load languoids/tree/**/md.ini into SQLite3 db, return filename."""
    loader = make_loader(root, with_raw=with_raw)
    dbfile = _backend.load(loader, rebuild=rebuild)
    return str(dbfile)


def make_loader(root, with_raw=True):
    if with_raw:  # import here to register models for create_all()
        from . import raw as _raw

    def load_func(conn):
        if with_raw:
            _raw.make_loader(root=root)(conn)
        _load(conn, root)

    return load_func


def _load(conn, root):
    insert_lang = sa.insert(Languoid, bind=conn).execute

    sa.insert(Macroarea, bind=conn).execute([{'name': n} for n in sorted(MACROAREA)])
    lang_ma = languoid_macroarea.insert(bind=conn).execute

    has_country = sa.select([sa.exists()
        .where(Country.id == sa.bindparam('id'))], bind=conn).scalar
    insert_country = sa.insert(Country, bind=conn).execute
    lang_country = languoid_country.insert(bind=conn).execute

    insert_link = sa.insert(Link, bind=conn).execute
    insert_source = sa.insert(Source, bind=conn).execute
    insert_altname = sa.insert(Altname, bind=conn).execute
    insert_trigger = sa.insert(Trigger, bind=conn).execute
    insert_ident = sa.insert(Identifier, bind=conn).execute
    insert_comment = sa.insert(ClassificationComment, bind=conn).execute
    insert_ref = sa.insert(ClassificationRef, bind=conn).execute
    insert_enda = sa.insert(Endangerment, bind=conn).execute
    insert_el = sa.insert(EthnologueComment, bind=conn).execute

    insert_ir = sa.insert(IsoRetirement, bind=conn).execute
    insert_irct = sa.insert(IsoRetirementChangeTo, bind=conn).execute

    for l in _languoids.iterlanguoids(root):
        lid = l['id']

        macroareas = l.pop('macroareas')
        countries = l.pop('countries')

        links = l.pop('links', None)
        sources = l.pop('sources', None)
        altnames = l.pop('altnames', None)
        triggers = l.pop('triggers', None)
        identifier = l.pop('identifier', None)
        classification = l.pop('classification', None)
        endangerment = l.pop('endangerment', None)
        hh_ethnologue_comment = l.pop('hh_ethnologue_comment', None)
        iso_retirement = l.pop('iso_retirement', None)

        insert_lang(l)
        for ma in macroareas:
            lang_ma(languoid_id=lid, macroarea_name=ma)
        for name, cc in countries:
            if not has_country(id=cc):
                insert_country(id=cc, name=name)
            lang_country(languoid_id=lid, country_id=cc)
        if links is not None:
            for i, link in enumerate(links, 1):
                insert_link(languoid_id=lid, ord=i, **link)
        if sources is not None:
            for provider, data in iteritems(sources):
                for i, s in enumerate(data, 1):
                    insert_source(languoid_id=lid, provider=provider, ord=i, **s)
        if altnames is not None:
            for provider, names in iteritems(altnames):
                for i, n in enumerate(names, 1):
                    insert_altname(languoid_id=lid, provider=provider, ord=i, **n)
        if triggers is not None:
            for field, triggers in iteritems(triggers):
                for i, t in enumerate(triggers, 1):
                    insert_trigger(languoid_id=lid, field=field, trigger=t, ord=i)
        if identifier is not None:
            for site, i in iteritems(identifier):
                insert_ident(languoid_id=lid, site=site, identifier=i)
        if classification is not None:
            for c, value in iteritems(classification):
                isref, kind = CLASSIFICATION[c]
                if isref:
                    for i, r in enumerate(value, 1):
                        insert_ref(languoid_id=lid, kind=kind, ord=i, **r)
                else:
                    insert_comment(languoid_id=lid, kind=kind, comment=value)
        if endangerment is not None:
            insert_enda(languoid_id=lid, **endangerment)
        if hh_ethnologue_comment is not None:
            insert_el(languoid_id=lid, **hh_ethnologue_comment)
        if iso_retirement is not None:
            change_to = iso_retirement.pop('change_to')
            insert_ir(languoid_id=lid, **iso_retirement)
            for i, c in enumerate(change_to, 1):
                insert_irct(languoid_id=lid, code=c, ord=i)
