from datapackage_pipelines.wrapper import spew, ingest

parameters, datapackage, res_iter = ingest()

amounts = [
    'net',
    'gross',
    'dedicated',
    'commitment_allowance',
    'personnel',
    'contractors',
    'amounts',
    'commitment_balance',
]

codes_and_titles = dict(
    ('admin_cls_code_%d' % l, 'admin_cls_title_%d' % l)
    for l in range(2,10,2)
)

phases = {
    'מקורי': 'allocated',
    'מאושר': 'revised',
    'ביצוע': 'executed'
}


resource = datapackage['resources'][0]
fields = resource['schema']['fields']
new_fields = [{
        'name': 'code',
        'type': 'string'
    },
    {
        'name': 'title',
        'type': 'string'
    }
]
for field in fields:
    name = field['name']
    if name in amounts:
        for phase in phases.values():
            new_fields.append({
                'name': name + '_' + phase,
                'type': field['type']
            })
    elif name in list(codes_and_titles.keys()):
        pass
    elif name in list(codes_and_titles.values()):
        pass
    elif name == 'phase':
        pass
    else:
        new_fields.append(field)
resource['schema']['fields'] = new_fields


def process_first(rows):
    for row in rows:
        phase_key = phases[row['phase']]
        del row['phase']

        for amount in amounts:
            row[amount + '_' + phase_key] = row[amount]
            del row[amount]

        save = {}
        for code_key, title_key in codes_and_titles.items():
            save[code_key] = row[code_key]
            save[title_key] = row[title_key]
            del row[code_key]
            del row[title_key]

        for code_key, title_key in codes_and_titles.items():
            row['code'] = save[code_key]
            row['title'] = save[title_key]
            yield row

def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_first(first)

    for res in res_iter_:
        yield res

spew(datapackage, process_resources(res_iter))
