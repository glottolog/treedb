# models.py - sqlalchemy schema, loading, checking, and queries for sqlite3 db

import sqlalchemy as sa

from sqlalchemy import (Table, Column, ForeignKey, CheckConstraint,
                        Integer, Float, String, Text, Enum, DateTime, Date,
                        UniqueConstraint, Index)

from sqlalchemy.orm import relationship, aliased

from .backend import Model

__all__ = ['LEVEL', 'Languoid']

FAMILY, LANGUAGE, DIALECT = LEVEL = ('family', 'language', 'dialect')

SPECIAL_FAMILIES = ('Sign Language',
                    'Unclassifiable',
                    'Pidgin',
                    'Unattested',
                    'Artificial Language',
                    'Mixed Language',
                    'Speech Register')

BOOKKEEPING = 'Bookkeeping'

MACROAREA = {'North America', 'South America',
             'Eurasia',
             'Africa',
             'Australia', 'Papunesia'}

LINK_SCHEME = {'https', 'http'}

SOURCE_PROVIDER = {'glottolog'}

ALTNAME_PROVIDER = {'multitree', 'lexvo', 'hhbib_lgcode',
                    'wals', 'wals other',
                    'moseley & asher (1994)', 'ruhlen (1987)',
                    'glottolog', 'ethnologue', 'elcat', 'aiatsis'}

TRIGGER_FIELD = {'lgcode', 'inlg'}

IDENTIFIER_SITE = {'multitree', 'endangeredlanguages',
                   'wals', 'languagelandscape'}

CLASSIFICATION = {'sub': (False, 'sub'), 'subrefs': (True, 'sub'),
                  'family': (False, 'family'), 'familyrefs': (True, 'family')}

CLASSIFICATION_KIND = {c for _, c in CLASSIFICATION.values()}

ENDANGERMENT_STATUS = ('not endangered',
                       'threatened', 'shifting',
                       'moribund', 'nearly extinct',
                       'extinct')

EL_COMMENT_TYPE = {'Missing', 'Spurious'}

ISORETIREMENT_REASON = {'split', 'merge', 'duplicate', 'non-existent', 'change'}


class Languoid(Model):

    __tablename__ = 'languoid'

    id = Column(String(8), CheckConstraint('length(id) = 8'), primary_key=True)

    name = Column(String, CheckConstraint("name != ''"),
                  nullable=False, unique=True)

    level = Column(Enum(*LEVEL), nullable=False)

    parent_id = Column(ForeignKey('languoid.id',
                                  deferrable=True, initially='DEFERRED'),
                       index=True)

    hid = Column(Text, CheckConstraint('length(hid) >= 3'), unique=True)
    iso639_3 = Column(String(3), CheckConstraint('length(iso639_3) = 3'),
                      unique=True)

    latitude = Column(Float, CheckConstraint('latitude BETWEEN -90 AND 90'))
    longitude = Column(Float, CheckConstraint('longitude BETWEEN -180 AND 180'))

    __table_args__ = (CheckConstraint('(latitude IS NULL)'
                                      ' = (longitude IS NULL)'),
                      {'info': {'without_rowid': True}})

    def __repr__(self):
        hid_iso = [f'{n}={getattr(self, n)!r}' for n in ('hid', 'iso639_3') if getattr(self, n)]
        hid_iso = ' '.join(hid_iso) if hid_iso else ''
        return (f'<{self.__class__.__name__}'
                f' id={self.id!r}'
                f' level={self.level!r}'
                f' name={self.name!r}'
                f'{hid_iso}>')

    parent = relationship('Languoid', remote_side=[id])

    children = relationship('Languoid', remote_side=[parent_id], order_by=id)

    macroareas = relationship('Macroarea',
                              secondary='languoid_macroarea',
                              order_by='Macroarea.name',
                              back_populates='languoids')

    countries = relationship('Country',
                             secondary='languoid_country',
                             order_by='Country.id',
                             back_populates='languoids')

    links = relationship('Link',
                         order_by='Link.ord',
                         back_populates='languoid')

    timespan = relationship('Timespan',
                            back_populates='languoid')

    sources = relationship('Source',
                           order_by='[Source.provider, Source.bibitem_id]',
                           back_populates='languoid')

    altnames = relationship('Altname',
                            order_by='[Altname.provider, Altname.name, Altname.lang]',
                            back_populates='languoid')

    triggers = relationship('Trigger',
                            order_by='[Trigger.field, Trigger.ord]',
                            back_populates='languoid')

    identifiers = relationship('Identifier',
                               order_by='Identifier.site',
                               back_populates='languoid')

    subclassificationcomment = relationship('ClassificationComment', uselist=False,
        primaryjoin="and_(ClassificationComment.languoid_id == Languoid.id, ClassificationComment.kind == 'sub')")

    subclassificationrefs = relationship('ClassificationRef', order_by='ClassificationRef.ord',
        primaryjoin="and_(ClassificationRef.languoid_id == Languoid.id, ClassificationRef.kind == 'sub')")

    familyclassificationcomment = relationship('ClassificationComment', uselist=False,
        primaryjoin="and_(ClassificationComment.languoid_id == Languoid.id, ClassificationComment.kind == 'family')")

    familyclassificationrefs = relationship('ClassificationRef', order_by='ClassificationRef.ord',
        primaryjoin="and_(ClassificationRef.languoid_id == Languoid.id, ClassificationRef.kind == 'family')")

    endangerment = relationship('Endangerment',
                                uselist=False,
                                back_populates='languoid')

    ethnologue_comment = relationship('EthnologueComment',
                                      uselist=False,
                                      back_populates='languoid')

    iso_retirement = relationship('IsoRetirement',
                                  uselist=False,
                                  back_populates='languoid')

    @classmethod
    def tree(cls, *, include_self=False, with_steps=False, with_terminal=False):
        Child, Parent = (aliased(cls, name=n) for n in ('child', 'parent'))

        tree_1 = sa.select([Child.id.label('child_id')])

        if include_self:
            tree_1_parent = Child
        else:
            tree_1_parent = Parent
            tree_1.append_from(sa.join(Child, Parent,
                                       Child.parent_id == Parent.id))

        tree_1.append_column(tree_1_parent.id.label('parent_id'))

        if with_steps:
            steps = 0 if include_self else 1
            tree_1.append_column(sa.literal(steps).label('steps'))

        if with_terminal:
            terminal = sa.type_coerce(tree_1_parent.parent_id == None,
                                      sa.Boolean)
            tree_1.append_column(terminal.label('terminal'))

        tree_1 = tree_1.cte('tree', recursive=True)

        tree_2 = sa.select([tree_1.c.child_id, Parent.parent_id])
        tree_2.append_from(tree_1.join(Parent,
                                       sa.and_(Parent.id == tree_1.c.parent_id,
                                               Parent.parent_id != None)))

        if with_steps:
            tree_2.append_column((tree_1.c.steps + 1).label('steps'))

        if with_terminal:
            Granny = aliased(Languoid, name='grandparent')

            tree_2.append_column((Granny.parent_id == None).label('terminal'))
            tree_2 = tree_2.select_from(tree_2.froms[-1]
                .outerjoin(Granny, Granny.id == Parent.parent_id))

        return tree_1.union_all(tree_2)

    @classmethod
    def _path_part(cls, label='path_part', include_self=True, bottomup=False, _tree=None):
        if _tree is None:
            tree = cls.tree(include_self=include_self, with_steps=True)
        else:
            tree = _tree

        select_path_part = sa.select([tree.c.parent_id.label(label)])\
                           .where(tree.c.child_id == cls.id)\
                           .correlate(cls)\
                           .order_by(tree.c.steps if bottomup
                                     else tree.c.steps.desc())

        return select_path_part

    @classmethod
    def path(cls, *, label='path', delimiter='/', include_self=True, bottomup=False, _tree=None):
        squery = cls._path_part(include_self=include_self, bottomup=bottomup, _tree=_tree)
        path = sa.func.group_concat(squery.c.path_part, delimiter).label(label)
        return sa.select([path]).label(label)

    @classmethod
    def child_root(cls, innerjoin=False, rightjoin=False):
        tree = Languoid.tree(with_terminal=True)

        Child, Root = (aliased(cls, name=n) for n in ('child', 'root'))

        is_child = (tree.c.child_id == Child.id)

        is_root = sa.and_(tree.c.parent_id == Root.id,
                          tree.c.terminal == True)

        if innerjoin:
            child_root = sa.join(Child, tree, is_child).join(Root, is_root)
        elif rightjoin:
            child_tree = sa.join(Child, tree, is_child)
            child_root = sa.outerjoin(Root, child_tree,
                                      sa.and_(is_root,
                                              Root.parent_id == None))
        else:
            tree_root = sa.join(tree, Root, is_root)
            child_root = sa.outerjoin(Child, tree_root, is_child)

        return Child, Root, child_root

    @classmethod
    def path_family_language(cls, *, path_label='path', path_delimiter='/', include_self=True, bottomup=False,
                             family_label='family_id', language_label='language_id'):
        tree = cls.tree(include_self=include_self, with_steps=True, with_terminal=True)

        path = cls.path(label=path_label, delimiter=path_delimiter, bottomup=bottomup, _tree=tree)

        family = sa.select([tree.c.parent_id])\
            .where(tree.c.child_id == cls.id)\
            .correlate(cls)\
            .where(tree.c.steps > 0)\
            .where(tree.c.terminal == True)

        Ancestor = aliased(Languoid, name='ancestor')

        language = sa.select([tree.c.parent_id])\
            .where(tree.c.child_id == cls.id)\
            .correlate(cls)\
            .where(cls.level == DIALECT)\
            .where(sa.exists()
                .where(Ancestor.id == tree.c.parent_id)
                .where(Ancestor.level == LANGUAGE))

        return path, family.label(family_label), language.label(language_label)


class Macroarea(Model):

    __tablename__ = 'macroarea'

    name = Column(Enum(*sorted(MACROAREA)), primary_key=True)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' {self.name!r}>')

    languoids = relationship('Languoid',
                             secondary='languoid_macroarea',
                             order_by='Languoid.id',
                             back_populates='macroareas')


languoid_macroarea = Table('languoid_macroarea', Model.metadata,
                           Column('languoid_id',
                                  ForeignKey('languoid.id'),
                                  primary_key=True),
                           Column('macroarea_name',
                                  ForeignKey('macroarea.name'),
                                  primary_key=True),
                           info={'without_rowid': True})


class Country(Model):

    __tablename__ = 'country'

    id = Column(String(2), CheckConstraint('length(id) = 2'), primary_key=True)

    name = Column(Text, CheckConstraint("name != ''"), nullable=False,
                  unique=True)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' id={self.id!r}'
                f' name={self.name!r}>')

    languoids = relationship('Languoid',
                             secondary='languoid_country',
                             order_by='Languoid.id',
                             back_populates='countries')

    @classmethod
    def printf(cls, *, label='printf'):
        return sa.func.printf('%s (%s)', cls.name, cls.id)

    @classmethod
    def jsonf(cls, *, label='jsonf'):
        return sa.func.json_object('id', cls.id,
                                   'name', cls.name).label(label)


languoid_country = Table('languoid_country', Model.metadata,
                         Column('languoid_id',
                                ForeignKey('languoid.id'),
                                primary_key=True),
                         Column('country_id',
                                ForeignKey('country.id'),
                                primary_key=True),
                         info={'without_rowid': True})


class Link(Model):

    __tablename__ = 'link'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    ord = Column(Integer, CheckConstraint('ord >= 1'), primary_key=True)

    url = Column(Text, CheckConstraint("url != ''"), nullable=False)

    title = Column(Text, CheckConstraint("title != ''"))
    scheme = Column(Text, Enum(*sorted(LINK_SCHEME)))

    __table_args__ = (UniqueConstraint(languoid_id, url),
                      CheckConstraint("substr(url, 1, length(scheme) + 3)"
                                      " = scheme || '://'"),
                      {'info': {'without_rowid': True}})

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' ord={self.ord!r}'
                f' url={self.url!r}'
                f' title={self.title!r}'
                f' scheme={self.schemme!r}>')

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='links')

    @classmethod
    def printf(cls, *, label='printf'):
        return sa.case([(cls.title != None,
                         sa.func.printf('[%s](%s)', cls.title, cls.url))],
                       else_=cls.url).label(label)

    @classmethod
    def jsonf(cls, *, label='jsonf'):
        return sa.func.json_object('scheme', cls.scheme,
                                   'url', cls.url,
                                   'title', cls.title).label(label)


class Timespan(Model):

    __tablename__ = 'timespan'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)

    start_year = Column(Integer,
                        CheckConstraint('start_year BETWEEN -999999 AND 999999'),
                        nullable=False)
    start_month = Column(Integer,
                         CheckConstraint('start_month BETWEEN 1 AND 12'),
                         nullable=False)
    start_day = Column(Integer,
                       CheckConstraint('start_day BETWEEN 1 AND 31'),
                       nullable=False)

    end_year = Column(Integer,
                      CheckConstraint('end_year BETWEEN -999999 AND 999999'),
                      nullable=False)
    end_month = Column(Integer,
                       CheckConstraint('end_month BETWEEN 1 AND 12'),
                       nullable=False)
    end_day = Column(Integer,
                     CheckConstraint('end_day BETWEEN 1 AND 31'),
                     nullable=False)

    __table_args__ = (CheckConstraint('end_year - start_year >= 0'),
                      {'info': {'without_rowid': True}})

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='timespan')

    @classmethod
    def jsonf(cls, *, label='jsonf'):
        return sa.func.json_object('start_year', cls.start_year,
                                   'start_month', cls.start_month,
                                   'start_day', cls.start_day,
                                   'end_year', cls.end_year,
                                   'end_month', cls.end_month,
                                   'end_day', cls.end_day).label(label)


class Source(Model):

    __tablename__ = 'source'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    provider = Column(Text, Enum(*sorted(SOURCE_PROVIDER)), primary_key=True)
    bibitem_id = Column(ForeignKey('bibitem.id'), primary_key=True)

    pages = Column(Text, CheckConstraint("pages != ''"))
    trigger = Column(Text, CheckConstraint("trigger != ''"))

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' provider={self.provider!r}'
                f' bibitem_id={self.bibitem_id!r}')

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='sources')

    bibitem = relationship('Bibitem',
                            innerjoin=True,
                            back_populates='sources')

    @classmethod
    def printf(cls, bibfile, bibitem, *, label='printf'):
        return sa.case([(sa.and_(cls.pages != None, cls.trigger != None),
                         sa.func.printf('**%s:%s**:%s<trigger "%s">',
                                        bibfile.name, bibitem.bibkey,
                                        cls.pages, cls.trigger)),
                        (cls.pages != None,
                         sa.func.printf('**%s:%s**:%s', bibfile.name,
                                                        bibitem.bibkey,
                                                        cls.pages)),
                        (cls.trigger != None,
                         sa.func.printf('**%s:%s**<trigger "%s">',
                                        bibfile.name, bibitem.bibkey,
                                        cls.trigger))],
                       else_=sa.func.printf('**%s:%s**', bibfile.name,
                                                         bibitem.bibkey)
                       ).label(label)

    @classmethod
    def jsonf(cls, bibfile, bibitem, *, label='jsonf'):
        return sa.func.json_object('bibfile', bibfile.name,
                                   'bibkey', bibitem.bibkey,
                                   'pages', cls.pages,
                                   'trigger', cls.trigger).label(label)


class Bibfile(Model):

    __tablename__ = 'bibfile'

    id = Column(Integer, primary_key=True)

    name = Column(String, CheckConstraint("name != ''"), nullable=False,
                  unique=True)

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' id={self.id!r}'
                f' name={self.name!r}')

    bibitems = relationship('Bibitem', back_populates='bibfile')


class Bibitem(Model):

    __tablename__ = 'bibitem'

    id = Column(Integer, primary_key=True)

    bibfile_id = Column(ForeignKey('bibfile.id'), nullable=False)
    bibkey = Column(Text, CheckConstraint("bibkey != ''"), nullable=False)

    __table_args__ = (UniqueConstraint(bibfile_id, bibkey),)

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' bibfile_id={self.bibfile_id!r}'
                f' bibkey={self.bibkey!r}')

    bibfile = relationship('Bibfile',
                           innerjoin=True,
                           back_populates='bibitems')

    sources = relationship('Source', back_populates='bibitem')

    classificationrefs = relationship('ClassificationRef',
                                      back_populates='bibitem')

    endangermentsources = relationship('EndangermentSource',
                                       back_populates='bibitem')


class Altname(Model):

    __tablename__ = 'altname'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    provider = Column(Text, Enum(*sorted(ALTNAME_PROVIDER)), primary_key=True)
    name = Column(Text, CheckConstraint("name != ''"), primary_key=True)
    lang = Column(String(3), CheckConstraint('length(lang) IN (0, 2, 3)'), primary_key=True)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' povider={self.provider!r}'
                f' name={self.name!r}>'
                f' lang={self.lang!r}')

    languoid = relationship('Languoid', innerjoin=True,
                            back_populates='altnames')

    @classmethod
    def printf(cls, *, label='printf'):
        return sa.case([(cls.lang == '', cls.name)],
                       else_=sa.func.printf('%s [%s]', cls.name,
                                                       cls.lang)
                       ).label(label)

    @classmethod
    def jsonf(cls, *, label='jsonf'):
        return sa.func.json_object('name', cls.name,
                                   'lang', cls.lang).label(label)


class Trigger(Model):

    __tablename__ = 'trigger'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    field = Column(Enum(*sorted(TRIGGER_FIELD)), primary_key=True)
    trigger = Column(Text, CheckConstraint("trigger != ''"), primary_key=True)

    ord = Column(Integer, CheckConstraint('ord >= 1'), nullable=False)

    __table_args__ = (UniqueConstraint(languoid_id, field, ord),
                      {'info': {'without_rowid': True}})

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' field={self.field!r}'
                f' trigger={self.trigger!r}>')

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='triggers')


class Identifier(Model):

    __tablename__ = 'identifier'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    site = Column(Enum(*sorted(IDENTIFIER_SITE)), primary_key=True)

    identifier = Column(Text, CheckConstraint("identifier != ''"), nullable=False)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' site={self.site!r}'
                f' identifier={self.identifier!r}>')

    languoid = relationship('Languoid', innerjoin=True,
                             back_populates='identifiers')


class ClassificationComment(Model):

    __tablename__ = 'classificationcomment'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    kind = Column(Enum(*sorted(CLASSIFICATION_KIND)), primary_key=True)

    comment = Column(Text, CheckConstraint("comment != ''"), nullable=False)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' kind={self.kind!r}'
                f' comment={self.comment!r}>')

    languoid = relationship('Languoid', innerjoin=True)


class ClassificationRef(Model):

    __tablename__ = 'classificationref'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    kind = Column(Enum(*sorted(CLASSIFICATION_KIND)), primary_key=True)
    bibitem_id = Column(ForeignKey('bibitem.id'), primary_key=True)

    ord = Column(Integer, CheckConstraint('ord >= 1'), nullable=False)

    pages = Column(Text, CheckConstraint("pages != ''"))

    __table_args__ = (UniqueConstraint(languoid_id, kind, ord),
                      {'info': {'without_rowid': True}})

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' kind={self.kind!r}'
                f' bibitem_id={self.bibitem_id!r}')

    languoid = relationship('Languoid', innerjoin=True)

    bibitem = relationship('Bibitem',
                           innerjoin=True,
                           back_populates='classificationrefs')

    @classmethod
    def printf(cls, bibfile, bibitem, *, label='printf'):
        return sa.func.printf(sa.case([(cls.pages != None, '**%s:%s**:%s')],
                                      else_='**%s:%s**'),
                              bibfile.name, bibitem.bibkey, cls.pages).label(label)

    @classmethod
    def jsonf(cls, bibfile, bibitem, *, label='jsonf'):
        return sa.func.json_object('bibfile', bibfile.name,
                                   'bibkey', bibitem.bibkey,
                                   'pages', cls.pages,
                                   'trigger', None).label(label)


class Endangerment(Model):

    __tablename__ = 'endangerment'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)

    status = Column(Enum(*ENDANGERMENT_STATUS), nullable=False)

    source_id = Column(ForeignKey('endangerment_source.id'), nullable=False)
    date = Column(DateTime, nullable=False)
    comment = Column(Text, CheckConstraint("comment != ''"), nullable=False)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' status={self.status!r}'
                f' source_id={self.source_id!r}'
                f' date={self.date!r}>')

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='endangerment')

    source = relationship('EndangermentSource',
                          innerjoin=True,
                          back_populates='endangerment')

    @classmethod
    def jsonf(cls, source, bibfile, bibitem, *, label='jsonf'):
        source = sa.func.json_object('name', source.name,
                                     'bibfile', bibfile.name,
                                     'bibkey', bibitem.bibkey,
                                     'pages', source.pages)
        return sa.func.json_object('status', cls.status,
                                   'source', source,
                                   'date', cls.date,
                                   'comment', cls.comment).label(label)


class EndangermentSource(Model):

    __tablename__ = 'endangerment_source'

    id = Column(Integer, primary_key=True)
    name = Column(Text, CheckConstraint("name != ''"), nullable=False, unique=True)

    bibitem_id = Column(ForeignKey('bibitem.id'))
    pages = Column(Text, CheckConstraint("pages != ''"))

    __table_args__ = (UniqueConstraint(bibitem_id, pages),
                      CheckConstraint('(bibitem_id IS NULL) = (pages IS NULL)'))

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' id={self.id!r}'
                f' name={self.name!r}'
                f' bibitem_id={self.bibitem_id!r}'
                f' pages={self.pages!r}>')

    bibitem = relationship('Bibitem',
                           back_populates='endangermentsources')

    endangerment = relationship('Endangerment',
                                uselist=False,
                                back_populates='source')

    @classmethod
    def printf(cls, bibfile, bibitem, *, label='printf'):
        return sa.case([(cls.bibitem_id == None, cls.name)],
                       else_=sa.func.printf('**%s:%s**:%s', bibfile.name,
                                                            bibitem.bibkey,
                                                            cls.pages)).label(label)


class EthnologueComment(Model):

    __tablename__ = 'ethnologuecomment'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)

    isohid = Column(Text, CheckConstraint('length(isohid) >= 3'), nullable=False)
    comment_type = Column(Enum(*sorted(EL_COMMENT_TYPE)), nullable=False)
    ethnologue_versions = Column(Text, CheckConstraint('length(ethnologue_versions) >= 3'), nullable=False)
    comment = Column(Text, CheckConstraint("comment != ''"), nullable=False)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' isohid={self.isohid!r}'
                f' comment_type={self.comment_type!r}'
                f' ethnologue_versions={self.ethnologue_versions!r}>')

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='ethnologue_comment')

    @classmethod
    def jsonf(cls, *, label='jsonf', optional=False):
        mapping = sa.func.json_object('isohid', cls.isohid,
                                      'comment_type', cls.comment_type,
                                      'ethnologue_versions', cls.ethnologue_versions,
                                      'comment', cls.comment)
        if optional:
            return sa.case([(cls.languoid_id == None, None)], else_=mapping).label(label)
        return mapping.label(label)


class IsoRetirement(Model):

    __tablename__ = 'isoretirement'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)

    code = Column(String(3), CheckConstraint('length(code) = 3'), nullable=False)
    name = Column(Text, CheckConstraint("name != ''"), nullable=False)

    change_request = Column(String(8), CheckConstraint("change_request LIKE '____-___' "))

    effective = Column(Date, nullable=False)
    reason = Column(Enum(*sorted(ISORETIREMENT_REASON)), nullable=False)

    remedy = Column(Text, CheckConstraint("remedy != ''"))
    comment = Column(Text, CheckConstraint("comment != ''"))

    __table_args__ = (
        # TODO: fix disagreement
        Index('change_request_key', sa.func.coalesce(change_request, effective)),
        CheckConstraint("remedy IS NOT NULL OR reason = 'non-existent'"),
        {'info': {'without_rowid': True}},
    )

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' code={self.code!r}'
                f' name={self.name!r}'
                f' change_request={self.change_request!r}'
                f' effective={self.effective!r}'
                f' reason={self.reason!r}'
                f' remedy={self.remedy!r}>')

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='iso_retirement')

    change_to = relationship('IsoRetirementChangeTo',
                             order_by='IsoRetirementChangeTo.ord',
                             back_populates='iso_retirement')

    @classmethod
    def jsonf(cls, *, change_to=None, label='jsonf', optional=False):
        mapping = sa.func.json_object('code', cls.code,
                                      'name', cls.name,
                                      'change_request', cls.change_request,
                                      'change_to', change_to,
                                      'effective', cls.effective,
                                      'reason', cls.reason,
                                      'remedy', cls.remedy,
                                      'comment', cls.comment)
        if optional:
            return sa.case([(cls.languoid_id == None, None)], else_=mapping).label(label)
        return mapping.label(label)


class IsoRetirementChangeTo(Model):

    __tablename__ = 'isoretirement_changeto'

    languoid_id = Column(ForeignKey('isoretirement.languoid_id'), primary_key=True)
    code = Column(String(3), CheckConstraint('length(code) = 3'), primary_key=True)

    ord = Column(Integer, CheckConstraint('ord >= 1'), nullable=False)

    __table_args__ = (UniqueConstraint('languoid_id', 'ord'),
                      {'info': {'without_rowid': True}})

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' code={self.code!r}>')

    iso_retirement = relationship('IsoRetirement',
                                  innerjoin=True,
                                  back_populates='change_to')
