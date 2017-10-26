import copy

from datapackage_pipelines.wrapper import spew, ingest
from decimal import Decimal

parameters, datapackage, res_iter = ingest()

last_completed_year = str(parameters['last-completed-year'])

amounts = [
    'net',
    'gross',
    'dedicated',
    'commitment_allowance',
    'commitment_balance',

    'personnel',
    'contractors',
    'amounts',
]
factors = [
    1000, 1000, 1000, 1000, 1000, 1, 1, 1,
]


codes_and_titles = [
    ('admin_cls_code_%d' % l, 'admin_cls_title_%d' % l)
    for l in range(2,10,2)
]

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
    },
    {
        'name': 'hierarchy',
        'type': 'array'
    },
    {
        'name': 'parent',
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
    elif name in list(dict(codes_and_titles).keys()):
        pass
    elif name in list(dict(codes_and_titles).values()):
        pass
    elif name == 'phase':
        pass
    else:
        new_fields.append(field)
resource['schema']['fields'] = new_fields


def process_row(row, phase_key):
    for amount, factor in zip(amounts, factors):
        value = row[amount]
        if value is not None and value.strip() != '':
            value = Decimal(row[amount]) * factor
        row[amount + '_' + phase_key] = value
        del row[amount]

    save = {}
    for code_key, title_key in codes_and_titles:
        save[code_key] = row[code_key]
        if len(save[code_key]) % 2 == 1:
            save[code_key] = '0' + save[code_key]
        save[title_key] = row[title_key]
        del row[code_key]
        del row[title_key]

    if int(save[codes_and_titles[0][0]]) != 0:
        hierarchy = [['00', 'המדינה']]
    else:
        hierarchy = []
    for i, (code_key, title_key) in enumerate(codes_and_titles):
        expected_length = i*2 + 4
        row['code'] = '0'*(expected_length-len(save[code_key])) + save[code_key]
        row['title'] = save[title_key] or 'לא ידוע'
        row['hierarchy'] = hierarchy
        row['parent'] = None if len(hierarchy) == 0 else hierarchy[-1][0]
        yield row
        hierarchy.append([row['code'], row['title']])

    row['hierarchy'] = None
    row['parent'] = None

    row['code'] = 'C%d' % (int(row['func_cls_code_1']),)
    row['title'] = '%s' % (row['func_cls_title_1'],)
    yield row

    row['code'] = 'C%d%02d' % (int(row['func_cls_code_1']), int(row['func_cls_code_2']))
    row['title'] = '%s/%s' % (row['func_cls_title_1'], row['func_cls_title_2'])
    yield row

    if not row['code'].startswith('0000'):
        row['code'] = '00'
        row['title'] = 'המדינה'
        row['hierarchy'] = []
        yield row


def process_first(rows):
    for i, row in enumerate(rows):
        phase_key = phases[row['phase']]
        del row['phase']

        row_ = copy.deepcopy(row)
        yield from process_row(row_, phase_key)

        if row['year'] > last_completed_year:
            yield from process_row(row, 'revised')


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_first(first)

    for res in res_iter_:
        yield res

spew(datapackage, process_resources(res_iter))
