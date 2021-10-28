import dataflows as DF
from decimal import Decimal

def datarecords(kind):
    return map(
        lambda r: r['value'],
        DF.Flow(
            DF.load(f'https://data-input.obudget.org/api/datarecords/{kind}', format='json', property='result')
        ).results()[0][0]
    )

def traverse(node, path):
    children = node.get('children')
    name = node.get('name')
    yield dict(path=path + [name])
    if children:
        for child in children:
            yield from traverse(child, path + [name])

def all_units():
    hierarchies = datarecords('hierarchy')
    yield dict(path=[])
    for node in hierarchies:
        yield from traverse(node, [])


def flow(*_):
    return DF.Flow(
        all_units(),
        DF.add_field('office', 'string', lambda r: r['path'][0] if len(r['path']) > 0 else None, **{'es:keyword': True}),
        DF.add_field('unit', 'string', lambda r: r['path'][1] if len(r['path']) > 1 else None, **{'es:keyword': True}),
        DF.add_field('subunit', 'string', lambda r: r['path'][2] if len(r['path']) > 2 else None, **{'es:keyword': True}),
        DF.add_field('subsubunit', 'string', lambda r: r['path'][3] if len(r['path']) > 3 else None, **{'es:keyword': True}),
        DF.add_field('breadcrumbs', 'string', lambda r: '/'.join(r['path']) or 'משרדי הממשלה', **{'es:exclude': True}),
        DF.add_field('id', 'string', lambda r: '__'.join(r['path']) or 'main', **{'es:exclude': True}),
        DF.delete_fields(['path', ]),
        DF.add_field('min_year', 'integer', 2020),
        DF.add_field('max_year', 'integer', 2020),
        DF.add_field('kind', 'string', 'gov_social_service_unit', **{'es:keyword': True, 'es:exclude': True}),
        DF.add_field('kind_he', 'string', 'שירותים חברתיים במיקור חוץ', **{'es:keyword': True, 'es:exclude': True}),
        DF.add_field('score', 'number', 1000, **{'es:score-column': True}),
        DF.set_primary_key(['kind', 'id']),
        DF.update_resource(-1, name='units', **{'dpp:streaming': True}),

        # Ensure we only have the main offices
        DF.filter_rows(lambda r: r['unit'] is None),
        DF.filter_rows(lambda r: r['office'] != 'משרד העליה והקליטה'),


        DF.dump_to_path('/var/datapackages/units/social_services'),
        DF.dump_to_sql(dict(
            units={'resource-name': 'units'}
        ))
    )


if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer(),
    ).process()
