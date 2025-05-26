import dataflows as DF

def unwind():
    def func(rows):
        for row in rows:
            ret_base = dict(
                year=row.get('year'),
                transaction_id=row.get('transaction_id'),
                date=row.get('date'),
                pending=row.get('is_pending'),
                req_title=row.get('req_title'),
                change_title=row.get('change_title'),
                change_type_name=row.get('change_type_name'),
                committee_id=row.get('committee_id'),
            )
            for item in row.get('change_list', []):
                budget_code_title = item.get('budget_code_title')
                budget_code, title =budget_code_title.split(':', 1)
                yield dict(
                    **ret_base,
                    budget_code=budget_code,
                    budget_title=title,
                    budget_item_description=item.get('ai_budget_item_description'),
                    change_explanation=item.get('ai_change_explanation'),
                    net_expense_diff=item.get('net_expense_diff'),
                    gross_expense_diff=item.get('gross_expense_diff'),
                    allocated_income_diff=item.get('allocated_income_diff'),
                    commitment_limit_diff=item.get('commitment_limit_diff'),
                    personnel_max_diff=item.get('personnel_max_diff'),
                    change_id=item.get('change_id')
                )
    return DF.Flow(
        DF.add_field(name='budget_code', type='string'),
        DF.add_field(name='budget_title', type='string'),
        DF.add_field(name='budget_item_description', type='string'),
        DF.add_field(name='change_explanation', type='string'),
        DF.add_field(name='net_expense_diff', type='number'),
        DF.add_field(name='gross_expense_diff', type='number'),
        DF.add_field(name='allocated_income_diff', type='number'),
        DF.add_field(name='commitment_limit_diff', type='number'),
        DF.add_field(name='personnel_max_diff', type='number'),
        DF.add_field(name='change_id', type='string'),
        DF.add_field(name='pending', type='boolean'),
        func,
        DF.select_fields([
            'year',
            'transaction_id',
            'req_title',
            'change_title',
            'change_type_name',
            'committee_id',
            'date',
            'change_id',
            'pending',
            'budget_code',
            'budget_title',
            'budget_item_description',
            'change_explanation',
            'net_expense_diff',
            'gross_expense_diff',
            'allocated_income_diff',
            'commitment_limit_diff',
            'personnel_max_diff',
        ]),
    )


def flow(params, *_):
    out_path = params['out']

    return DF.Flow(
        unwind(),
        DF.set_primary_key(None),
        DF.dump_to_path(out_path),
        DF.update_resource(-1, **{'dpp:streaming': True}),    
    )