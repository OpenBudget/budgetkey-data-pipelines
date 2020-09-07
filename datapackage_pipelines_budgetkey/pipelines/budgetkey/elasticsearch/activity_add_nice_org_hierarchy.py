import dataflows as DF

def process_history(row):
    if 'history' in row and 'publisher_name' in row:
        for x in row['history']:
            nice_hierarchy = [row['publisher_name']]
            nice_hierarchy.extend(
                x.get(f) for f in ('unit', 'subunit', 'subsubunit')
            )
            nice_hierarchy = [x for x in nice_hierarchy if x]
            nice_hierarchy = '/'.join(nice_hierarchy)
            x['nice_org_hierarchy'] = nice_hierarchy

def update_schema(package: DF.PackageWrapper):
    for resource in package.pkg.resources:
        for field in resource.descriptor['schema']['fields']:
            if field['name'] == 'history':
                field['es:schema']['fields'].append(dict(
                    dict(name='nice_org_hierarchy', type='string'),
                ))
    yield package.pkg
    yield from package


def flow(*_):
    return DF.Flow(
        process_history,
        update_schema
    )