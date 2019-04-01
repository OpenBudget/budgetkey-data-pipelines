from datapackage_pipelines.wrapper import process
import datapackage

ROOF_TITLES = datapackage.Package('https://next.obudget.org/datapackages/budget/national/processed/roof-names/datapackage.json')
ROOF_TITLES = ROOF_TITLES.resources[0].iter(keyed=True)
ROOF_TITLES = dict(
    ((x['year'], x['code']), x['title'])
    for x in ROOF_TITLES
)


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    if spec['name'] == 'national-budget-changes':
        roofs = {}
        for change in row['change_list']:
            code = change['budget_code_title'][:4]
            amount = sum(
                change.get(x, 0)
                for x in [
                    'net_expense_diff',
                    'gross_expense_diff',
                    'allocated_income_diff',
                    'commitment_limit_diff',
                ]
            )
            roofs.setdefault(code, 0)
            roofs[code] += amount
        roofs = sorted(roofs.items(), key=lambda r: -abs(r[1]))
        for r in roofs:
            r.append(ROOF_TITLES.get((row['year'], r[0]), r[0]))
        row['summary'] = {
            'title': row['req_title'][0],
            'kind': row['change_title'][0],
            'from': list(filter(lambda r: r[1] <= 0, roofs)),
            'to': list(filter(lambda r: r[1] >= 0, roofs)),
        }

    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'summary',
        'type': 'object',
        'es:index': False
    })
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)

