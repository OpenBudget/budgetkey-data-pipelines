import os
# from sqlalchemy import create_engine, text

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.format_number import format_number, format_percentage

import logging

# engine = create_engine(os.environ['DPP_DB_ENGINE'])
# conn = engine.connect()

def entity_kinds_mapper(entries, field):
    MAP = [
        ('association', 'עמותות'),
        ('company', 'חברות'),
        ('municipality', 'רשויות מקומיות'),
        ('other', 'פרטיים ולא מזוהה'),
        ('rest', 'אחר')
    ]
    kinds = [x[0] for x in MAP]
    out = [0.0] * len(MAP)
    for entry in entries:
        kind = entry.get('kind', 'rest')
        if kind not in kinds:
            kind = 'rest'
        index = kinds.index(kind)
        out[index] = (out[index] or 0) + (entry.get(field) or 0)
    out = [
        (MAP[i][1], out[i])
        for i in range(len(MAP))
        if out[i] > 0
    ]
    return out

def get_history_charts(row):
    top_recipients = sorted(row['recipients'], key=lambda x: x['total_paid'], reverse=True)[:10]
    entity_kinds_approved = entity_kinds_mapper(row['entity_kinds'], 'total_approved')
    entity_kinds_paid = entity_kinds_mapper(row['entity_kinds'], 'total_paid')
    entity_kinds_count = entity_kinds_mapper(row['entity_kinds'], 'count')
    return dict(
        title='ניתוח היסטורי',
        subcharts=[
            dict(
                title='סכומים ששולמו ומיצוי התמיכות',
                long_title='היסטוריית התמיכות בתכנית בהיבטי כספים שאושרו, שולמו ומיצוי התמיכות',
                type='plotly',
                layout=dict(
                    xaxis=dict(title='שנה', type='category'),
                    yaxis=dict(
                        title='סכום (₪)',
                        separatethousands=True,
                        rangemode='tozero'
                    ),
                    yaxis2=dict(
                        title='מיצוי התמיכות',
                        overlaying='y',
                        side='right',
                        tickformat='.0%',
                        rangemode='tozero'
                    ),
                    margin=dict(r=50),
                ),
                chart=[
                    dict(
                        mode='lines+markers',
                        name='סכום שאושר',
                        x=[year for year in range(row['min_year'], row['max_year'] + 1)],
                        y=[row['per_year'].get(str(year), {}).get('approved', 0)
                        for year in range(row['min_year'], row['max_year'] + 1)],
                    ),
                    dict(
                        mode='lines+markers',
                        name='סכום ששולם',
                        x=[year for year in range(row['min_year'], row['max_year'] + 1)],
                        y=[row['per_year'].get(str(year), {}).get('paid', 0)
                        for year in range(row['min_year'], row['max_year'] + 1)],
                    ),
                    dict(
                        mode='lines+markers',
                        name='מיצוי התמיכות הממוצע',
                        x=[year for year in range(row['min_year'], row['max_year'] + 1)],
                        y=[row['per_year'].get(str(year), {}).get('average_utilization', None)
                        for year in range(row['min_year'], row['max_year'] + 1)],
                        yaxis='y2',
                    )
                ],
            ),
            dict(
                title='מקבלי התמיכה העיקריים בתכנית זו',
                long_title='התמיכות במקבלי התמיכה העיקריים בתכנית זו, לאורך כל השנים',
                description='הסכום המוצג הוא סך התמיכות ששולמו בכל שנה בתכנית זו לעשרת מקבלי התמיכה העיקריים בתכנית זו',
                type='plotly',
                layout=dict(
                    xaxis=dict(title='שנה', type='category'),
                    yaxis=dict(
                        title='סכום (₪)',
                        separatethousands=True,
                        rangemode='tozero'
                    )
                ),
                chart=[
                    dict(
                        mode='lines+markers',
                        name='{}'.format(x['name']),
                        x=[year for year in range(row['min_year'], row['max_year'] + 1)],
                        y=[x['per_year'].get(str(year), {}).get('paid', 0)
                        for year in range(row['min_year'], row['max_year'] + 1)],
                    )
                    for x in top_recipients
                ]
            ),
            dict(
                title='סוגי מקבלי התמיכה בתכנית זו',
                long_title='התפלגות סוגי מקבלי התמיכה, לפי סוג מקבל התמיכה, בכל שנות הפעילות',
                type='plotly',
                layout=dict(
                    grid=dict(rows=1, columns=3)
                ),
                chart=[
                    dict(
                        type='pie',
                        hoverinfo='label+percent+value',
                        labels=[x[0] for x in entity_kinds_approved],
                        values=[x[1] for x in entity_kinds_approved],
                        name='סך הכספים שאושרו',
                        domain=dict(row=0, column=0),
                        title=dict(
                            text='סך הכספים שאושרו',
                            position='bottom center'
                        )
                    ),
                    dict(
                        type='pie',
                        hoverinfo='label+percent+value',
                        labels=[x[0] for x in entity_kinds_paid],
                        values=[x[1] for x in entity_kinds_paid],
                        name='סך הכספים ששולמו',
                        domain=dict(row=0, column=1),
                        title=dict(
                            text='סך הכספים ששולמו',
                            position='bottom center'
                        )
                    ),
                    dict(
                        type='pie',
                        hoverinfo='label+percent+value',
                        labels=[x[0] for x in entity_kinds_count],
                        values=[x[1] for x in entity_kinds_count],
                        name='סך מקבלי התמיכה',
                        domain=dict(row=0, column=2),
                        title=dict(
                            text='סך מקבלי התמיכה',
                            position='bottom center'
                        )
                    )
                ]
            )
        ]
    )

def get_entity_charts(row):
    charts = dict(
        title='מקבלי התמיכה בתכנית זו',
        subcharts=[
            dict(
                title=title,
                long_title='ארגונים אשר אושרה להם תמיכה בתוכנית זו, {} לאורך כל השנים'.format(title),
                type='adamkey',
                chart=dict(
                    values=[dict(
                        label='<a href="/i/org/{}/{}?theme=budgetkey">{}</a>'.format(x['kind'], x['id'], x['name']),
                        amount=x[field],
                        amount_fmt=formatter(x[field]),
                    )
                    for x in sorted(row['recipients'], key=lambda x: x[field] if x[field] is not None else -1, reverse=True)],
                    selected=0
                )
            ) for title, field, formatter in [
                ('לפי סך הכספים שאושרו', 'total_approved', format_number),
                ('לפי סך הכספים ששולמו', 'total_paid', format_number),
                ('לפי המיצוי הממוצע של התמיכות', 'average_utilization', format_percentage),
            ]
        ]
    )
    return charts

def process_row(row, *_):
    row['charts'] = [
        get_history_charts(row),
        get_entity_charts(row),
    ]
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append(
        {
            'name': 'charts',
            'type': 'array',
            'es:itemType': 'object',
            'es:index': False
        }
    )
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
