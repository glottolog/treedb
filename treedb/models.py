"""Main ``sqlalchemy`` schema for SQLite3 database."""

import typing

import sqlalchemy as sa

from sqlalchemy import (Table, Column, ForeignKey, CheckConstraint,
                        Integer, Float, String, Text, Enum, DateTime, Date,
                        UniqueConstraint, Index)

from sqlalchemy.orm import relationship, aliased

from . import _globals
from ._globals import REGISTRY as registry

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

LINK_SCHEME = {'https', 'http'}

SOURCE_PROVIDER = {'glottolog'}

ALTNAME_PROVIDER = {'multitree', 'lexvo', 'hhbib_lgcode',
                    'wals', 'wals other',
                    'moseley & asher (1994)', 'ruhlen (1987)',
                    'glottolog', 'ethnologue', 'elcat', 'aiatsis'}

TRIGGER_FIELD = {'lgcode', 'inlg'}

IDENTIFIER_SITE = {'multitree', 'endangeredlanguages',
                   'wals', 'languagelandscape'}

_SUB, _FAMILY = 'sub', 'family'

CLASSIFICATION = {'sub': (False, _SUB), 'subrefs': (True, _SUB),
                  'family': (False, _FAMILY), 'familyrefs': (True, _FAMILY)}

CLASSIFICATION_KIND = {c for _, c in CLASSIFICATION.values()}

EL_COMMENT_TYPE = {'Missing', 'Spurious'}

ISORETIREMENT_REASON = {'split', 'merge', 'duplicate', 'non-existent', 'change'}


# Windows, Python < 3.9: https://www.sqlite.org/download.html
def json_object(*, sort_keys_: bool,
                label_: typing.Optional[str] = None, **kwargs):
    items = sorted(kwargs.items()) if sort_keys_ else kwargs.items()
    obj = sa.func.json_object(*[x for kv in items for x in kv])
    return obj.label(label_) if label_ is not None else obj


def json_datetime(date):
    date = sa.func.replace(date, ' ', 'T')
    return sa.func.replace(date, '.000000', '')


@registry.mapped
class Languoid:

    __tablename__ = 'languoid'

    id = Column(String(8), CheckConstraint('length(id) = 8'), primary_key=True)

    name = Column(String, CheckConstraint("name != ''"),
                  nullable=False, unique=True)

    level = Column(ForeignKey('languoidlevel.name'), nullable=False)

    parent_id = Column(ForeignKey('languoid.id',
                                  # do not require to insert parent before child
                                  deferrable=True, initially='DEFERRED'),
                       index=True)

    hid = Column(Text, CheckConstraint('length(hid) >= 3'), unique=True)
    iso639_3 = Column(String(3), CheckConstraint('length(iso639_3) = 3'),
                      unique=True)

    latitude = Column(Float, CheckConstraint('latitude BETWEEN -90 AND 90'))
    longitude = Column(Float, CheckConstraint('longitude BETWEEN -180 AND 180'))

    __table_args__ = (CheckConstraint('(latitude IS NULL) = (longitude IS NULL)'),
                      {'info': {'without_rowid': True}})

    def __repr__(self):
        hid_iso = [f'{n}={getattr(self, n)!r}' for n in ('hid', 'iso639_3') if getattr(self, n)]
        hid_iso = ' '.join(hid_iso) if hid_iso else ''
        return (f'<{self.__class__.__name__}'
                f' id={self.id!r}'
                f' level={self.level!r}'
                f' name={self.name!r}'
                f'{hid_iso}>')

    languoidlevel = relationship('LanguoidLevel',
                                 back_populates='languoids')

    pseudofamily = relationship('PseudoFamily',
                                primaryjoin='PseudoFamily.languoid_id == Languoid.id',
                                back_populates='languoid')

    parent = relationship('Languoid', remote_side=[id])

    children = relationship('Languoid', remote_side=[parent_id], order_by=id,
                            viewonly=True)

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
                           order_by='[Source.provider_id, Source.bibitem_id]',
                           back_populates='languoid')

    altnames = relationship('Altname',
                            order_by='[Altname.provider_id, Altname.name, Altname.lang]',
                            back_populates='languoid')

    triggers = relationship('Trigger',
                            order_by='[Trigger.field, Trigger.ord]',
                            back_populates='languoid')

    identifiers = relationship('Identifier',
                               order_by='Identifier.site_id',
                               back_populates='languoid')

    subclassificationcomment = relationship('ClassificationComment', uselist=False,
        primaryjoin="and_(ClassificationComment.languoid_id == Languoid.id,"
                    " ClassificationComment.kind == 'sub')",
        viewonly=True)

    subclassificationrefs = relationship('ClassificationRef', order_by='ClassificationRef.ord',
        primaryjoin="and_(ClassificationRef.languoid_id == Languoid.id,"
                    "ClassificationRef.kind == 'sub')",
        viewonly=True)

    familyclassificationcomment = relationship('ClassificationComment', uselist=False,
        primaryjoin="and_(ClassificationComment.languoid_id == Languoid.id,"
                    " ClassificationComment.kind == 'family')",
        viewonly=True)

    familyclassificationrefs = relationship('ClassificationRef', order_by='ClassificationRef.ord',
        primaryjoin="and_(ClassificationRef.languoid_id == Languoid.id,"
                    "ClassificationRef.kind == 'family')",
        viewonly=True)

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
    def _aliased_child_parent(cls, *,
                              child_root: bool = False,
                              parent_root: bool = False):
        if child_root and parent_root:  # pragma: no cover
            raise ValueError('child_root and parent_root are mutually exclusive')

        Child = aliased(cls, name='root' if child_root else 'child')
        Parent = aliased(cls, name='root' if parent_root else 'parent')
        return Child, Parent

    @classmethod
    def _tree(cls, *, from_parent: bool = False,
              innerjoin=False,
              child_root=None, parent_root=None, node_level=None,
              with_steps: bool = False,
              with_terminal: bool = False):
        if innerjoin not in (False, True, 'reflexive'):  # pragma: no cover
            raise ValueError(f'invalid innerjoin: {innerjoin!r}')

        Child, Parent = cls._aliased_child_parent(child_root=child_root,
                                                  parent_root=parent_root)

        if from_parent:
            Node, Relative = Parent, Child
            node_label, relative_label = 'parent_id', 'child_id'
            join_source, join_target = Node.id, Relative.parent_id
            recurse_relative = Relative.id
        else:
            Node, Relative = Child, Parent
            node_label, relative_label = 'child_id', 'parent_id'
            join_source, join_target = Node.parent_id, Relative.id
            recurse_relative = Relative.parent_id

        tree_1 = sa.select(Node.id.label(node_label))

        if innerjoin == 'reflexive':
            tree_1_relative = Node
        else:
            tree_1_relative = Relative
            tree_1 = tree_1.join_from(Node, Relative,
                                      join_source == join_target,
                                      isouter=not innerjoin)

        tree_1 = tree_1.add_columns(tree_1_relative.id.label(relative_label))

        if with_steps:
            steps = 0 if innerjoin == 'reflexive' else 1
            tree_1 = tree_1.add_columns(sa.literal(steps).label('steps'))

        if with_terminal:
            if from_parent:  # pragma: no cover
                raise NotImplementedError
            tree_1_terminal = Node if innerjoin == 'reflexive' else Relative
            terminal = sa.type_coerce(tree_1_terminal.parent_id == None, sa.Boolean)
            tree_1 = tree_1.add_columns(terminal.label('terminal'))

        if child_root is not None:
            tree_1 = tree_1.where(Child.parent_id == None if child_root else
                                  Child.parent_id != None)
        if parent_root is not None:
            tree_1 = tree_1.where(Parent.parent_id == None if parent_root else
                                  Parent.parent_id != None)

        if node_level is not None:
            if node_level not in LEVEL:  # pragma: no cover
                raise ValueError(f'invalid node_level: {node_level!r}')
            tree_1 = tree_1.where(Node.level == node_level)

        tree_1 = tree_1.cte('tree', recursive=True)

        tree_2 = sa.select(tree_1.c[node_label],
                           recurse_relative.label(relative_label))

        tree_2_onclause = (tree_1.c[relative_label] == join_target)
        if not from_parent:
            tree_2_onclause = sa.and_(tree_2_onclause, recurse_relative != None)

        tree_2_fromclause = tree_1.join(Relative, tree_2_onclause)

        if with_steps:
            tree_2 = tree_2.add_columns((tree_1.c.steps + 1).label('steps'))

        if with_terminal:
            GrandRelative = aliased(cls, name='grand' + ('child'
                                                         if from_parent else
                                                         'parent'))
            if from_parent:  # pragma: no cover
                raise NotImplementedError
            tree_2 = tree_2.add_columns((GrandRelative.parent_id == None).label('terminal'))
            tree_2_fromclause = tree_2_fromclause.outerjoin(GrandRelative,
                                                            Relative.parent_id
                                                            == GrandRelative.id)

        tree_2 = tree_2.select_from(tree_2_fromclause)

        tree = tree_1.union_all(tree_2)

        return tree

    @classmethod
    def tree(cls, *, include_self: bool = False,
             with_steps: bool = False,
             with_terminal: bool = False):
        return cls._tree(from_parent=False,
                         innerjoin='reflexive' if include_self else True,
                         with_steps=with_steps, with_terminal=with_terminal)

    @classmethod
    def _path_part(cls, label: str = 'path_part',
                   include_self: bool = True,
                   bottomup: bool = False,
                   _tree=None):
        if _tree is None:
            tree = cls.tree(include_self=include_self, with_steps=True)
        else:
            tree = _tree

        select_path_part = (sa.select(tree.c.parent_id.label(label))
                            .where(tree.c.child_id == cls.id)
                            .correlate(cls)
                            .order_by(tree.c.steps if bottomup
                                      else tree.c.steps.desc())
                            .alias('parent_path'))

        return select_path_part

    @classmethod
    def path(cls, *, label: str = 'path',
             delimiter: str = _globals.FILE_PATH_SEP,
             include_self: bool = True,
             bottomup: bool = False,
             _tree=None):
        squery = cls._path_part(include_self=include_self, bottomup=bottomup,
                                _tree=_tree)
        path = sa.func.group_concat(squery.c.path_part, delimiter).label(label)
        return sa.select(path).label(label)

    @classmethod
    def node_relative(cls, *, from_parent: bool = False,
                      innerjoin=False,
                      child_root=None, parent_root=None, node_level=None,
                      with_steps: bool = False,
                      with_terminal: bool = False):
        tree = cls._tree(from_parent=from_parent, innerjoin=innerjoin,
                         child_root=child_root, parent_root=parent_root,
                         node_level=node_level,
                         with_steps=with_steps, with_terminal=with_terminal)

        Child, Parent = cls._aliased_child_parent(child_root=child_root,
                                                  parent_root=parent_root)

        if from_parent:
            Node, Relative = Parent, Child
            node_label, relative_label = 'parent_id', 'child_id'
        else:
            Node, Relative = Child, Parent
            node_label, relative_label = 'child_id', 'parent_id'

        del Child, Parent

        is_node = (tree.c[node_label] == Node.id)
        is_relative = (tree.c[relative_label] == Relative.id)

        node_relative = (tree.join(Node, is_node)
                         .join(Relative, is_relative,
                               isouter=not innerjoin))

        return Node, Relative, tree, node_relative

    @classmethod
    def child_ancestor(cls, *, innerjoin=False,
                       child_level=None):
        Child, Parent, _, child_parent = cls.node_relative(from_parent=False,
                                                           innerjoin=innerjoin,
                                                           node_level=child_level)
        return Child, Parent, _, child_parent

    @classmethod
    def parent_descendant(cls, *, innerjoin=False,
                          parent_root=None, parent_level=None):
        Parent, Child, _, parent_child = cls.node_relative(from_parent=True,
                                                           innerjoin=innerjoin,
                                                           parent_root=parent_root,
                                                           node_level=parent_level)
        return Parent, Child, parent_child

    @classmethod
    def path_family_language(cls, *, path_label: str = 'path',
                             path_delimiter: str = _globals.FILE_PATH_SEP,
                             include_self: bool = True,
                             bottomup: bool = False,
                             family_label: str = 'family_id',
                             language_label: str = 'language_id'):
        tree = cls.tree(include_self=include_self, with_steps=True, with_terminal=True)

        path = cls.path(label=path_label, delimiter=path_delimiter, bottomup=bottomup, _tree=tree)

        family = (sa.select(tree.c.parent_id)
                  .where(tree.c.child_id == cls.id)
                  .correlate(cls)
                  .where(tree.c.steps > 0)
                  .where(tree.c.terminal == True)
                  .label(family_label))

        Ancestor = aliased(Languoid, name='ancestor')

        language = (sa.select(tree.c.parent_id)
                    .where(tree.c.child_id == cls.id)
                    .correlate(cls)
                    .where(cls.level == DIALECT)
                    .where(sa.exists()
                           .where(tree.c.parent_id == Ancestor.id)
                           .where(Ancestor.level == LANGUAGE))
                    .label(language_label))

        return path, family, language


@registry.mapped
class LanguoidLevel:

    __tablename__ = 'languoidlevel'

    name = Column(String, CheckConstraint("name != ''"), primary_key=True)

    description = Column(Text, CheckConstraint("description != ''"),
                         nullable=False)

    ordinal = Column(Integer, CheckConstraint('ordinal >= 1'), nullable=False)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' name={self.name!r}'
                f' description={self.description!r}'
                f' ordinal={self.ordinal}>')

    languoids = relationship('Languoid',
                             back_populates='languoidlevel')


@registry.mapped
class PseudoFamily:

    __tablename__ = 'pseudofamily'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)

    name = Column(ForeignKey('languoid.name'), nullable=False,
                  unique=True)

    config_section = Column(String, CheckConstraint("config_section != ''"),
                            nullable=False, unique=True)

    description = Column(Text, CheckConstraint("description != ''"))

    bookkeeping = Column(sa.Boolean(create_constraint=False), CheckConstraint('bookkeeping = 1'),
                         unique=True)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' name={self.name!r}'
                f' config_section={self.config_section!r}'
                f' description={self.description!r}'
                f' bookkeeping={self.bookkeeping!r}>')

    languoid = relationship('Languoid', foreign_keys=[languoid_id],
                            back_populates='pseudofamily')


@registry.mapped
class Macroarea:

    __tablename__ = 'macroarea'

    name = Column(String, CheckConstraint("name != ''"), primary_key=True)

    config_section = Column(String, CheckConstraint("config_section != ''"),
                            nullable=False, unique=True)

    description = Column(Text, CheckConstraint("description != ''"),
                         nullable=False)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' name={self.name!r}'
                f' config_section={self.config_section!r}'
                f' description={self.description!r}>')

    languoids = relationship('Languoid',
                             secondary='languoid_macroarea',
                             order_by='Languoid.id',
                             back_populates='macroareas')


languoid_macroarea = Table('languoid_macroarea', registry.metadata,
                           Column('languoid_id',
                                  ForeignKey('languoid.id'),
                                  primary_key=True),
                           Column('macroarea_name',
                                  ForeignKey('macroarea.name'),
                                  primary_key=True),
                           info={'without_rowid': True})


@registry.mapped
class Country:

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
    def printf(cls, *, minimal: bool = True, label: str = 'printf', ):
        return (sa.func.printf('%s (%s)', cls.name, cls.id)
                if not minimal else cls.id).label(label)

    @classmethod
    def jsonf(cls, *, sort_keys: bool, label: str = 'jsonf'):
        return json_object(id=cls.id,
                           name=cls.name,
                           sort_keys_=sort_keys,
                           label_=label,)


languoid_country = Table('languoid_country', registry.metadata,
                         Column('languoid_id',
                                ForeignKey('languoid.id'),
                                primary_key=True),
                         Column('country_id',
                                ForeignKey('country.id'),
                                primary_key=True),
                         info={'without_rowid': True})


@registry.mapped
class Link:

    __tablename__ = 'link'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    ord = Column(Integer, CheckConstraint('ord >= 1'), primary_key=True)

    url = Column(Text, CheckConstraint("url != ''"), nullable=False)

    title = Column(Text, CheckConstraint("title != ''"))
    scheme = Column(Text, Enum(*sorted(LINK_SCHEME), create_constraint=True))

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
                f' scheme={self.scheme!r}>')

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='links')

    @classmethod
    def printf(cls, *, label: str = 'printf'):
        return sa.case((cls.title != None,
                        sa.func.printf('[%s](%s)', cls.title, cls.url)),
                       else_=cls.url).label(label)

    @classmethod
    def jsonf(cls, *, sort_keys: bool, label: str = 'jsonf'):
        return json_object(scheme=cls.scheme,
                           url=cls.url,
                           title=cls.title,
                           sort_keys_=sort_keys,
                           label_=label)


@registry.mapped
class Timespan:

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

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' start_year={self.start_year!r}'
                f' start_month={self.start_month!r}'
                f' start_day={self.start_day!r}'
                f' end_year={self.end_year!r}'
                f' end_month={self.end_month!r}'
                f' end_day={self.end_day!r}>')

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='timespan')

    @classmethod
    def jsonf(cls, *, sort_keys: bool, label: str = 'jsonf'):
        return json_object(start_year=cls.start_year,
                           start_month=cls.start_month,
                           start_day=cls.start_day,
                           end_year=cls.end_year,
                           end_month=cls.end_month,
                           end_day=cls.end_day,
                           sort_keys_=sort_keys,
                           label_=label)


@registry.mapped
class Source:

    __tablename__ = 'source'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    provider_id = Column(ForeignKey('sourceprovider.id'), primary_key=True)
    bibitem_id = Column(ForeignKey('bibitem.id'), primary_key=True)

    pages = Column(Text, CheckConstraint("pages != ''"))
    trigger = Column(Text, CheckConstraint("trigger != ''"))

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' provider_id={self.provider_id!r}'
                f' bibitem_id={self.bibitem_id!r}>')

    languoid = relationship('Languoid',
                            innerjoin=True,
                            back_populates='sources')

    provider = relationship('SourceProvider',
                            innerjoin=True,
                            back_populates='sources')

    bibitem = relationship('Bibitem',
                           innerjoin=True,
                           back_populates='sources')

    @classmethod
    def printf(cls, bibfile, bibitem,
               *, label: str = 'printf'):
        return sa.case((sa.and_(cls.pages != None, cls.trigger != None),
                        sa.func.printf('**%s:%s**:%s<trigger "%s">',
                                       bibfile.name, bibitem.bibkey,
                                       cls.pages, cls.trigger)),
                       (cls.pages != None,
                        sa.func.printf('**%s:%s**:%s',
                                       bibfile.name, bibitem.bibkey,
                                       cls.pages)),
                       (cls.trigger != None,
                        sa.func.printf('**%s:%s**<trigger "%s">',
                                       bibfile.name, bibitem.bibkey,
                                       cls.trigger)),
                       else_=sa.func.printf('**%s:%s**',
                                            bibfile.name, bibitem.bibkey)
                       ).label(label)

    @classmethod
    def jsonf(cls, bibfile, bibitem,
              *, sort_keys: bool, label: str = 'jsonf'):
        return json_object(bibfile=bibfile.name,
                           bibkey=bibitem.bibkey,
                           pages=cls.pages,
                           trigger=cls.trigger,
                           sort_keys_=sort_keys,
                           label_=label)


@registry.mapped
class SourceProvider:

    __tablename__ = 'sourceprovider'

    id = Column(Integer, primary_key=True)

    name = Column(Text, Enum(*sorted(SOURCE_PROVIDER), create_constraint=True),
                  nullable=False, unique=True)

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' name={self.name!r}>')

    sources = relationship('Source', back_populates='provider')


@registry.mapped
class Bibfile:

    __tablename__ = 'bibfile'

    id = Column(Integer, primary_key=True)

    name = Column(String, CheckConstraint("name != ''"), nullable=False,
                  unique=True)

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' id={self.id!r}'
                f' name={self.name!r}>')

    bibitems = relationship('Bibitem', back_populates='bibfile')


@registry.mapped
class Bibitem:

    __tablename__ = 'bibitem'

    id = Column(Integer, primary_key=True)

    bibfile_id = Column(ForeignKey('bibfile.id'), nullable=False)
    bibkey = Column(Text, CheckConstraint("bibkey != ''"), nullable=False)

    __table_args__ = (UniqueConstraint(bibfile_id, bibkey),)

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' bibfile_id={self.bibfile_id!r}'
                f' bibkey={self.bibkey!r}>')

    bibfile = relationship('Bibfile',
                           innerjoin=True,
                           back_populates='bibitems')

    sources = relationship('Source', back_populates='bibitem')

    classificationrefs = relationship('ClassificationRef',
                                      back_populates='bibitem')

    endangermentstatus = relationship('EndangermentStatus',
                                      back_populates='bibitem')

    endangermentsources = relationship('EndangermentSource',
                                       back_populates='bibitem')


@registry.mapped
class Altname:

    __tablename__ = 'altname'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    provider_id = Column(ForeignKey('altnameprovider.id'), primary_key=True)
    name = Column(Text, CheckConstraint("name != ''"), primary_key=True)
    lang = Column(String(3), CheckConstraint('length(lang) IN (0, 2, 3)'),
                  server_default='', primary_key=True)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' provider_id={self.provider_id!r}'
                f' name={self.name!r}'
                f' lang={self.lang!r}>')

    languoid = relationship('Languoid', innerjoin=True,
                            back_populates='altnames')

    provider = relationship('AltnameProvider',
                            innerjoin=True,
                            back_populates='altnames')

    @classmethod
    def printf(cls, *, label: str = 'printf'):
        full = sa.func.printf('%s [%s]', cls.name, cls.lang)
        return sa.case((cls.lang == '', cls.name), else_=full).label(label)

    @classmethod
    def jsonf(cls, *, sort_keys: bool, label: str = 'jsonf'):
        half = json_object(name=cls.name,
                           lang=None,
                           sort_keys_=sort_keys)
        full = json_object(name=cls.name,
                           lang=cls.lang,
                           sort_keys_=sort_keys)
        return sa.case((cls.lang == '', half), else_=full).label(label)


@registry.mapped
class AltnameProvider:

    __tablename__ = 'altnameprovider'

    id = Column(Integer, primary_key=True)

    name = Column(Text, Enum(*sorted(ALTNAME_PROVIDER), create_constraint=True),
                  nullable=False, unique=True)

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' name={self.name!r}>')

    altnames = relationship('Altname', back_populates='provider')


@registry.mapped
class Trigger:

    __tablename__ = 'trigger'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    field = Column(Enum(*sorted(TRIGGER_FIELD), create_constraint=True),
                   primary_key=True)
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


@registry.mapped
class Identifier:

    __tablename__ = 'identifier'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    site_id = Column(ForeignKey('identifiersite.id'), primary_key=True)

    identifier = Column(Text, CheckConstraint("identifier != ''"), nullable=False)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' site_id={self.site_id!r}'
                f' identifier={self.identifier!r}>')

    languoid = relationship('Languoid', innerjoin=True,
                             back_populates='identifiers')

    site = relationship('IdentifierSite', innerjoin=True,
                        back_populates='identifiers')


@registry.mapped
class IdentifierSite:

    __tablename__ = 'identifiersite'

    id = Column(Integer, primary_key=True)

    name = Column(Text, Enum(*sorted(IDENTIFIER_SITE), create_constraint=True),
                  nullable=False, unique=True)

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' name={self.name!r}>')

    identifiers = relationship('Identifier', back_populates='site')


@registry.mapped
class ClassificationComment:

    __tablename__ = 'classificationcomment'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    kind = Column(Enum(*sorted(CLASSIFICATION_KIND), create_constraint=True),
                  primary_key=True)

    comment = Column(Text, CheckConstraint("comment != ''"), nullable=False)

    __table_args__ = {'info': {'without_rowid': True}}

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' kind={self.kind!r}'
                f' comment={self.comment!r}>')

    languoid = relationship('Languoid', innerjoin=True)


@registry.mapped
class ClassificationRef:

    __tablename__ = 'classificationref'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)
    kind = Column(Enum(*sorted(CLASSIFICATION_KIND), create_constraint=True),
                  primary_key=True)
    bibitem_id = Column(ForeignKey('bibitem.id'), primary_key=True)

    ord = Column(Integer, CheckConstraint('ord >= 1'), nullable=False)

    pages = Column(Text, CheckConstraint("pages != ''"))

    __table_args__ = (UniqueConstraint(languoid_id, kind, ord),
                      {'info': {'without_rowid': True}})

    def __repr__(self):
        return (f'<{self.__class__.__name__}'
                f' languoid_id={self.languoid_id!r}'
                f' kind={self.kind!r}'
                f' bibitem_id={self.bibitem_id!r}>')

    languoid = relationship('Languoid', innerjoin=True)

    bibitem = relationship('Bibitem',
                           innerjoin=True,
                           back_populates='classificationrefs')

    @classmethod
    def printf(cls, bibfile, bibitem,
               *, label: str = 'printf'):
        return sa.func.printf(sa.case((cls.pages != None, '**%s:%s**:%s'),
                                      else_='**%s:%s**'),
                              bibfile.name, bibitem.bibkey, cls.pages).label(label)

    @classmethod
    def jsonf(cls, bibfile, bibitem,
              *, sort_keys: bool, label: str = 'jsonf'):
        return json_object(bibfile=bibfile.name,
                           bibkey=bibitem.bibkey,
                           pages=cls.pages,
                           trigger=None,
                           sort_keys_=sort_keys,
                           label_=label)


@registry.mapped
class Endangerment:

    __tablename__ = 'endangerment'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)

    status = Column(ForeignKey('endangermentstatus.name'), nullable=False)

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

    endangermentstatus = relationship('EndangermentStatus',
                                      innerjoin=True,
                                      back_populates='endangerments')

    source = relationship('EndangermentSource',
                          innerjoin=True,
                          back_populates='endangerment')

    @classmethod
    def jsonf(cls, source, bibfile, bibitem,
              *, sort_keys: bool, label: str = 'jsonf'):
        source = json_object(name=source.name,
                             bibfile=bibfile.name,
                             bibkey=bibitem.bibkey,
                             pages=source.pages,
                             sort_keys_=sort_keys)
        return json_object(status=cls.status,
                           source=source,
                           date=json_datetime(cls.date),
                           comment=cls.comment,
                           sort_keys_=sort_keys,
                           label_=label)


@registry.mapped
class EndangermentStatus:

    __tablename__ = 'endangermentstatus'

    name = Column(String, CheckConstraint("name != ''"), primary_key=True)

    config_section = Column(String, CheckConstraint("config_section != ''"),
                            nullable=False, unique=True)

    ordinal = Column(Integer, CheckConstraint('ordinal >= 1'), nullable=False)

    egids = Column(String, CheckConstraint("egids != ''"), nullable=False)
    unesco = Column(String, CheckConstraint("unesco != ''"), nullable=False)
    elcat = Column(String, CheckConstraint("elcat != ''"), nullable=False)
    icon = Column(String, CheckConstraint("icon != ''"), nullable=False)

    bibitem_id = Column(ForeignKey('bibitem.id'))

    __table_args__ = {'info': {'without_rowid': True}}

    bibitem = relationship('Bibitem',
                           back_populates='endangermentstatus')

    endangerments = relationship('Endangerment',
                                 back_populates='endangermentstatus')


@registry.mapped
class EndangermentSource:

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
    def printf(cls, bibfile, bibitem,
               *, label: str = 'printf'):
        return sa.case((cls.bibitem_id == None, cls.name),
                       else_=sa.func.printf('**%s:%s**:%s',
                                            bibfile.name, bibitem.bibkey,
                                            cls.pages)).label(label)


@registry.mapped
class EthnologueComment:

    __tablename__ = 'ethnologuecomment'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)

    isohid = Column(Text, CheckConstraint('length(isohid) >= 3'), nullable=False)
    comment_type = Column(Enum(*sorted(EL_COMMENT_TYPE), create_constraint=True),
                          nullable=False)
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
    def jsonf(cls, *, sort_keys: bool, optional: bool = False,
              label: str = 'jsonf', ):
        mapping = json_object(isohid=cls.isohid,
                              comment_type=cls.comment_type,
                              ethnologue_versions=cls.ethnologue_versions,
                              comment=cls.comment,
                              sort_keys_=sort_keys)
        if optional:
            return sa.case((cls.languoid_id == None, None), else_=mapping).label(label)
        return mapping.label(label)


@registry.mapped
class IsoRetirement:

    __tablename__ = 'isoretirement'

    languoid_id = Column(ForeignKey('languoid.id'), primary_key=True)

    code = Column(String(3), CheckConstraint('length(code) = 3'), nullable=False)
    name = Column(Text, CheckConstraint("name != ''"), nullable=False)

    change_request = Column(String(8), CheckConstraint("change_request LIKE '____-___' "))

    effective = Column(Date, nullable=False)
    reason = Column(Enum(*sorted(ISORETIREMENT_REASON), create_constraint=True),
                    nullable=False)

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
    def jsonf(cls, *, sort_keys: bool,
              change_to=None, optional: bool = False,
              label: str = 'jsonf'):
        mapping = json_object(code=cls.code,
                              name=cls.name,
                              change_request=cls.change_request,
                              change_to=change_to,
                              effective=json_datetime(cls.effective),
                              reason=cls.reason,
                              remedy=cls.remedy,
                              comment=cls.comment,
                              sort_keys_=sort_keys)
        if optional:
            return sa.case((cls.languoid_id == None, None), else_=mapping).label(label)
        return mapping.label(label)


@registry.mapped
class IsoRetirementChangeTo:

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
