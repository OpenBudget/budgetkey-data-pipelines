import os
# from sqlalchemy import create_engine, text

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.format_number import format_number, format_percentage

import logging

# engine = create_engine(os.environ['DPP_DB_ENGINE'])
# conn = engine.connect()


def get_history_charts(row):
    top_recipients = sorted(row['recipients'], key=lambda x: x['total_approved'], reverse=True)[:10]
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
                        tickformat='%',
                        rangemode='tozero'
                    )
                ),
                chart=[
                    dict(
                        mode='lines+markers',
                        name='סכום שאושר',
                        x=[year for year in range(row['min_year'], row['max_year'] + 1)],
                        y=[row['per_year'].get(year, {}).get('approved', 0)
                        for year in range(row['min_year'], row['max_year'] + 1)],
                    ),
                    dict(
                        mode='lines+markers',
                        name='סכום ששולם',
                        x=[year for year in range(row['min_year'], row['max_year'] + 1)],
                        y=[row['per_year'].get(year, {}).get('paid', 0)
                        for year in range(row['min_year'], row['max_year'] + 1)],
                    ),
                    dict(
                        mode='lines+markers',
                        name='מיצוי התמיכות הממוצע',
                        x=[year for year in range(row['min_year'], row['max_year'] + 1)],
                        y=[row['per_year'].get(year, {}).get('average_utilization', 0)
                        for year in range(row['min_year'], row['max_year'] + 1)],
                        yaxis='y2',
                    )
                ],
            ),
            dict(
                title='מקבלי התמיכה העיקריים בתכנית זו',
                long_title='התמיכות במקבלי התמיכה העיקריים בתכנית זו, לאורך כל השנים',
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
                        y=[x['per_year'].get(year, {}).get('total_approved', 0)
                        for year in range(row['min_year'], row['max_year'] + 1)],
                    )
                    for x in top_recipients
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
                    for x in sorted(row['recipients'], key=lambda x: x[field], reverse=True)],
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
