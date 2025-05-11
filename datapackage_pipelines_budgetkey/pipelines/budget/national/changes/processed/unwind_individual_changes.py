import dataflows as DF

            # dict(
            #     name='item_url',
            #     description='''
            #         קישור לדו״ח מפורט על התכניתה התקציבית באתר מפתח התקציב.
            #     ''',
            #     type='string',
            #     default=lambda row: f'https://next.obudget.org/dashboards/budget-transfers/budget/{row["budget_code"]}/{row["year"]}'
            # ),
            # dict(
            #     name='year',
            #     description='''
            #         השנה בה נשלחה לוועדת הכספים הבקשה שכוללת את השינוי הזה.
            #     ''',
            #     sample_values=[2017, 2020, 2025],
            #     type='integer',
            #     default=lambda row: row.get('year')
            # ),
            # dict(
            #     name='code',
            #     description='''
            #         מזהה הסעיף התקציבי שבו נעשה השינוי.
            #         (נקרא גם ״קוד התכנית התקציבית״).
            #         כשמציגים את רשימת השינויים בבקשת העברה, יש להציג תמיר את השדה הזה.
            #     ''',
            #     sample_values=['20.67.01', '26.13.01', '19.42.02'],
            #     type='string',
            #     default=lambda row: clean_budget_code(row.get('budget_code'))
            # ),
            # dict(
            #     name='title',
            #     description='''
            #         כותרת הסעיף התקציבי שבו נעשה השינוי.
            #         (נקרא גם ״שם התכנית התקציבית״).
            #         כשמציגים את רשימת השינויים בבקשת העברה, יש להציג תמיר את השדה הזה.
            #     ''',
            #     sample_values=['תכנון ובנייה', 'משרד הבריאות', 'משרד החינוך'],
            #     type='string',
            #     default=lambda row: row.get('budget_title')
            # ),
            # dict(
            #     name='transaction_id',
            #     description='''
            #         מזהה ייחודי של הבקשה בה מופיע השינוי.
            #     ''',
            #     sample_values=['2005/01-001', '2017/60-003', '2011/18-028'],
            #     type='string',
            #     default=lambda row: row.get('transaction_id')
            # ),
            # dict(
            #     name='is_pending',
            #     description='''
            #         האם השינוי עדיין ממתין לאישור.
            #     ''',
            #     possible_values=[True, False],
            #     type='boolean',
            #     default=lambda row: row.get('pending')
            # ),
            # dict(
            #     name='appproval_date',
            #     description='''
            #         תאריך אישור השינוי, במידה והוא אושר.
            #     ''',
            #     sample_values=['2021-01-01', '2023-12-31', '2025-02-29'],
            #     type='date',
            #     default=lambda row: row.get('date')
            # ),
            # dict(
            #     name='diff__amount_allocated',
            #     description='''
            #         השינוי בתקציב שהוקצה לסעיף.
            #     ''',
            #     sample_values=[1000000, -5000000, 10000000],
            #     type='number',
            #     default=lambda row: row.get('net_expense_diff')
            # ),

def unwind():
    def func(rows):
        for row in rows:
            ret_base = dict(
                year=row.get('year'),
                transaction_id=row.get('transaction_id'),
                date=row.get('date'),
                pending=row.get('is_pending'),
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
                )
    return DF.Flow(
        DF.add_field(name='budget_code', type='string'),
        DF.add_field(name='budget_title', type='string'),
        DF.add_field(name='budget_item_description', type='string'),
        DF.add_field(name='change_explanation', type='string'),
        DF.add_field(name='net_expense_diff', type='number'),
        DF.add_field(name='pending', type='boolean'),
        func,
        DF.select_fields([
            'year',
            'transaction_id',
            'date',
            'budget_code',
            'budget_title',
            'budget_item_description',
            'change_explanation',
            'net_expense_diff',
            'pending'
        ]),
    )


def flow(params, *_):
    out_path = params['out']

    return DF.Flow(
        unwind(),
        DF.dump_to_path(out_path),
        DF.update_resource(-1, **{'dpp:streaming': True}),    
    )