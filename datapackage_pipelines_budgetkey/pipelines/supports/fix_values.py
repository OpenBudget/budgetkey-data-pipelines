import logging
from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    if len(row['budget_code']) != 8:
        row['budget_code'] = '9' * 8
    if row['year_paid'] == 'אינו מוקצה':
        row['year_paid'] = ''
    row['budget_code'] = '00' + row['budget_code']
    return row


process(process_row=process_row)