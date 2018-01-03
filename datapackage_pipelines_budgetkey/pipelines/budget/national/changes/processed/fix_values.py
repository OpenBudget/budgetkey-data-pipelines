from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    if spec['name'] == 'national-budget-changes':
        row['change_list'] = dict(
            (k,v)
            for k, v in row.items()
            if k in {'budget_code_title',
                     'net_expense_diff',
                     'gross_expense_diff',
                     'allocated_income_diff',
                     'commitment_limit_diff',
                     'personnel_max_diff'}
        )

    return row


def modify_datapackage(dp, *_):
    for resource in dp['resources']:
        if resource['name'] == 'national-budget-changes':
            resource['schema']['fields'].append({
                'name': 'change_list',
                'type': 'object',
                'es:index': False
            })
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
