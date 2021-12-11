import re

import pytest
import sqlalchemy as sa

import treedb.models as _models

BOOK = 'book1242'

WALI = 'wali1263'

TREE = {BOOK: [(BOOK, BOOK, 0, True)],
        WALI: [(WALI, WALI, 0, False),
               (WALI, 'sout3209', 1, False),
               (WALI, 'cent2291', 2, False),
               (WALI, 'daga1276', 3, False),
               (WALI, 'safa1246', 4, False),
               (WALI, 'nort3234', 5, False),
               (WALI, 'nucl1748', 6, False),
               (WALI, 'west2461', 7, False),
               (WALI, 'gurm1247', 8, False),
               (WALI, 'nucl1743', 9, False),
               (WALI, 'otiv1239', 10, False),
               (WALI, 'bwam1248', 11, False),
               (WALI, 'nort2777', 12, False),
               (WALI, 'cent2243', 13, False),
               (WALI, 'gura1261', 14, False),
               (WALI, 'nort3149', 15, False),
               (WALI, 'volt1241', 16, False),
               (WALI, 'atla1278', 17, True)]}

FULL = {'include_self': True, 'with_steps': True, 'with_terminal': True}

EXCLUSIVE = {'with_steps': True, 'with_terminal': True}


@pytest.mark.parametrize(
    'child_id, parent_id, kwargs, expected',
    [pytest.param(BOOK, None, FULL, TREE[BOOK],
                  id=f'child_id={BOOK}, include_self'),
     pytest.param(BOOK, None, EXCLUSIVE, [],
                  id=f'child_id={BOOK}'),
     pytest.param(BOOK, None, {}, [],
                  id=f'child_id={BOOK}'),
     pytest.param(WALI, None, FULL, TREE[WALI],
                  id=f'child_id={WALI}, include_self'),
     pytest.param(WALI, None, EXCLUSIVE, TREE[WALI][1:],
                  id=f'child_id={WALI}'),
     pytest.param(WALI, None, {}, [(c, p) for c, p, _, _ in TREE[WALI][1:]],
                  id=f'child_id={WALI}')])
def test_languoid_tree(treedb, child_id, parent_id, kwargs, expected):
    tree = treedb.Languoid.tree(**kwargs)

    select_tree = tree.select()

    if child_id is not None:
        select_tree = select_tree.where(tree.c.child_id == child_id)

    if parent_id is not None:
        select_tree = select_tree.where(tree.c.parent_id == parent_id)

    with treedb.connect() as conn:
        result = conn.execute(select_tree).all()

    assert result == expected


@pytest.mark.parametrize(
    'model, whereclause, expected_repr',
    [pytest.param(_models.Languoid, _models.Languoid.id == '3adt1234',
                  r"<Languoid id='3adt1234' level='dialect' name='3Ad-Tekles'>",
                  id='model=Languoid(id=3adt1234)'),
     pytest.param(_models.LanguoidLevel, _models.LanguoidLevel.name == 'language',
                  r"<LanguoidLevel name='language' description='[^']+' ordinal=\d+>",
                  id='model=LanguoidLevel(name=language)'),
     pytest.param(_models.PseudoFamily, _models.PseudoFamily.name == 'Bookkeeping',
                  r"<PseudoFamily languoid_id='book1242' name='Bookkeeping'"
                  r" config_section='bookkeeping' description='[^']+' bookkeeping=True>",
                  id='model=PseudoFamily(name=Bookkeeping)'),
     pytest.param(_models.Macroarea, _models.Macroarea.name == 'Eurasia',
                  r"<Macroarea name='Eurasia' config_section='[^']+' description='[^']+'>",
                  id='model=Marcoarea(name=Eurasia)'),
     pytest.param(_models.Country, _models.Country.id == 'RU',
                  r"<Country id='RU' name='Russian Federation'>",
                  id='model=Country(id=RU)'),
     pytest.param(_models.Link, _models.Link.title != sa.null(),
                  r"<Link languoid_id='\w+' ord=\d+ url='[^']+' title='[^']+' scheme='https?'>",
                  id='model=Link(title)'),
     pytest.param(_models.Timespan, None,
                  r"<Timespan start_year=\d+ start_month=\d+ start_day=\d+"
                  r" end_year=\d+ end_month=\d+ end_day=\d+>",
                  id='model=Timespan',
                  marks=pytest.mark.xfail_glottolog_tag('v4.1', reason='no timespan in Glottolog v4.1',
                                                        raises=AssertionError)),
     pytest.param(_models.Source, None,
                  r"<Source languoid_id='\w+' provider_id=\d+ bibitem_id=\d+>",
                  id='model=Source'),
     pytest.param(_models.SourceProvider, _models.SourceProvider.name == 'glottolog',
                  r"<SourceProvider name='glottolog'>",
                  id='model=SourceProvider(name=glottolog)'),
     pytest.param(_models.Bibfile, _models.Bibfile.name == 'hh',
                  r"<Bibfile id=\d+ name='hh'>",
                  id='model=Bibfile(name=hh)'),
     pytest.param(_models.Bibitem, None,
                  r"<Bibitem bibfile_id=\d+ bibkey='[^']+'>",
                  id='model=Bibitem'),
     pytest.param(_models.Altname, _models.Altname.lang != sa.null(),
                  r"<Altname languoid_id='\w+' provider_id=\d+ name='[^']+' lang='[^']*'>",
                  id='model=Altname(lang)'),
     pytest.param(_models.AltnameProvider, _models.AltnameProvider.name == 'multitree',
                  r"<AltnameProvider name='multitree'>",
                  id='model=AltnameProvider(name=multitree)'),
     pytest.param(_models.Trigger, None,
                  r"<Trigger languoid_id='\w+' field='[^']+' trigger='[^']+'>",
                  id='model=Trigger'),
     pytest.param(_models.Identifier, None,
                  r"<Identifier languoid_id='\w+' site_id=\d+ identifier='[^']+'>",
                  id='model=Identifier'),
     pytest.param(_models.IdentifierSite, _models.IdentifierSite.name == 'multitree',
                  "<IdentifierSite name='multitree'>",
                  id='model=IdentifierSite(name=multitree)'),
     pytest.param(_models.ClassificationComment, None,
                  r"<ClassificationComment languoid_id='\w+' kind='\w+' comment='[^']+'>",
                  id='model=ClassificatrionComment'),
     pytest.param(_models.ClassificationRef, None,
                  r"<ClassificationRef languoid_id='\w+' kind='\w+' bibitem_id=\d+>",
                  id='model=ClassificationRef'),
     pytest.param(_models.Endangerment, None,
                  r"<Endangerment languoid_id='\w+' status='[^']+'"
                  r" source_id=\d+ date=datetime\.datetime\([^)]+\)>",
                  id='model=Endangerment'),
     pytest.param(_models.EndangermentSource, _models.EndangermentSource.name == 'E22',
                  r"<EndangermentSource id=\d+ name='E22' full_name='Ethnologue 22'"
                  r" bibitem_id=\d+ pages=None url='https://www.ethnologue.com/'>",
                  id='model=EndangermentSource(name=E22)'),
     pytest.param(_models.EthnologueComment, None,
                  r"<EthnologueComment languoid_id='\w+' isohid='[^']+' comment_type='[^']+'"
                  r" ethnologue_versions='[^']+'>",
                  id='model=EthnologueComment'),
     pytest.param(_models.IsoRetirement, None,
                  r"<IsoRetirement languoid_id='\w+' code='\w+' name='[^']+'"
                  r" change_request='[^']+' effective=datetime\.date\([^)]*\)"
                  r" reason='[^']*' remedy='[^']*'>",
                  id='model=IsoRetirement'),
     pytest.param(_models.IsoRetirementChangeTo, None,
                  r"<IsoRetirementChangeTo languoid_id='\w+' code='\w+'>",
                  id='model=IsoRetirementChangeTo')])
def test_repr(treedb, model, whereclause, expected_repr):
    query = sa.select(model)
    if whereclause is not None:
        query = query.where(whereclause)

    with treedb.Session() as session:
        inst = session.execute(query).scalars().first()

    result = repr(inst)

    assert re.fullmatch(expected_repr, result)
