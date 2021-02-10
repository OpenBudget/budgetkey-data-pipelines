import copy
import logging

from datapackage import Package

from datapackage_pipelines.wrapper import spew, ingest
from decimal import Decimal

parameters, datapackage, res_iter = ingest()

last_completed_year = parameters['last-completed-year']

amounts = [
    'net',
    'gross',
    'dedicated',
    'commitment_allowance',
    'commitment_balance',

    'personnel',
    'contractors',
    'amounts',
    'covid19_expenses'
]
factors = [
    1000, 1000, 1000, 1000, 1000, 1, 1, 1,
]

budget_fixes = Package('/var/datapackages/budget/national/changes/current-year-fixes/datapackage.json')
budget_fixes = list(budget_fixes.resources[0].iter(keyed=True))
budget_fixes = dict(
    ((x['year'], x['code']), x)
    for x in budget_fixes
)
logging.info('GOT %s budget fixes', len(budget_fixes))

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
    },
    {
        'name': 'depth',
        'type': 'integer'
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
    budget_fix = {}
    if phase_key == 'revised':
        program_code = row['admin_cls_code_6']
        program_code = '0'*(8-len(program_code)) + program_code
        budget_fix = budget_fixes.pop((row['year'], program_code), {})
        if budget_fix:
            logging.info('FIXING BUDGET %s, %s', program_code, budget_fix)

    row_amounts = {}
    fixed_amounts = {}

    for amount, factor in zip(amounts, factors):
        value = row[amount]
        if value is None:
            value = Decimal(0)
        if isinstance(value, Decimal):
            value *= factor
        key = amount + '_' + phase_key
        row_amounts[key] = value
        fixed_amounts[key] = value
        if budget_fix and value and budget_fix.get(amount):
            fixed_amounts[key] = value + budget_fix.get(amount)
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
        row.update(fixed_amounts if expected_length < 10 else row_amounts)
        row['code'] = '0'*(expected_length-len(save[code_key])) + save[code_key]
        row['title'] = save[title_key] or 'לא ידוע'
        row['hierarchy'] = hierarchy
        row['parent'] = None if len(hierarchy) == 0 else hierarchy[-1][0]
        row['depth'] = i+1
        yield row
        hierarchy.append([row['code'], row['title']])

    row['parent'] = None
    row['hierarchy'] = []

    row.update(fixed_amounts)

    if (
        (not
            (row['code'].startswith('0000') or
             row['code'].startswith('0089') or
             row['code'].startswith('0091') or 
             row['code'].startswith('0094') or 
             row['code'].startswith('0095') or
             row['code'].startswith('0098') or
             row['code'].startswith('0084')
            )
        ) or (row['econ_cls_code_2'] == '266')
       ):
        row['code'] = '00'
        row['title'] = 'המדינה'
        row['depth'] = 0
        yield row

    if (
        (not
            (row['code'].startswith('0000') or
             row['code'].startswith('0089') or
             row['code'].startswith('0091') or 
             row['code'].startswith('0094') or 
             row['code'].startswith('0095') or
             row['code'].startswith('0098')
            )
        ) or (row['econ_cls_code_2'] == '266')
       ):
        row['code'] = 'C%d' % (int(row['func_cls_code_1']),)
        row['title'] = '%s' % (row['func_cls_title_1'],)
        row['hierarchy'] = [['00', 'המדינה']]
        row['depth'] = -2
        yield row

        row['parent'] = row['code']
        row['hierarchy'].append([row['code'], row['title']])
        row['code'] = 'C%d%02d' % (int(row['func_cls_code_1']), int(row['func_cls_code_2']))
        row['title'] = '%s / %s' % (row['func_cls_title_1'], row['func_cls_title_2'])
        row['depth'] = -1
        yield row



def process_first(rows):
    for i, row in enumerate(rows):
        phase_key = phases[row['phase']]
        del row['phase']

        try:
            row_ = copy.deepcopy(row)
            yield from process_row(row_, phase_key)

            if row['year'] > last_completed_year:
                yield from process_row(row, 'revised')
        except:
            logging.exception('Offending row %r', row)
            raise


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_first(first)

    for res in res_iter_:
        yield res

spew(datapackage, process_resources(res_iter))
