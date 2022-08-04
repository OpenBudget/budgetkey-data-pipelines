import csv 
csv.field_size_limit(512 * 1024)


import dataflows as DF


def add_charts():
    def func(row):
        code = row['code']
        history = row['history']
        if history and len(history) > 0:
            history = sorted(history, key=lambda x: x['year'])
            layout = dict(
                xaxis=dict(
                    title='שנה',
                    type='category'
                ),
                yaxis=dict(
                    title='תקציב ב-₪',
                    rangemode='tozero',
                    separatethousands=True,
                )
            )
            data = []
            for measure, name in (
                    ('allocated', 'תקציב מקורי'),
                    ('revised', 'אחרי שינויים'),
                    ('executed', 'ביצוע בפועל')
            ):
                values = [h.get(measure) for h in history]
                trace = dict(
                    x=[int(h['year']) for h in history],
                    y=values,
                    mode='lines+markers',
                    name=name
                )
                data.append(trace)
            chart = dict(
                title='היסטוריה תקציבית',
                long_title='ערכי התקציב לאורך השנים (כפי שהופיעו בספרי התקציב)',
                type='plotly',
                chart=data,
                layout=layout
            )
            row['charts'].append(chart)
    return func


def flow(*_):
    return DF.Flow(
        DF.add_field('charts', 'array', default=[], **{'es:itemType': 'object', 'index': False}),
        add_charts(),
    )

if __name__ == '__main__':
    sample = DF.Flow(
        DF.load('/var/datapackages/budget/municipal/datacity-budgets/datapackage.json'),
        flow(),
        DF.printer()
    ).results()[0][0][1000]
    print(sample)