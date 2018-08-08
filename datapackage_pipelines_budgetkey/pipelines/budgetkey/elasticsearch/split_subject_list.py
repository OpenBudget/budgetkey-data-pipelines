import itertools
import logging

from datapackage_pipelines_budgetkey.common.periods import convert_period

from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    if row.get('subjects'):
        row['subject_list_keywords'] = [x.strip() for x in row['subjects'].split(';')]
    else:
        row['subject_list_keywords'] = []
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {
            'name': 'subject_list_keywords',
            'type': 'array',
            'es:itemType': 'string',
            'es:title': True,
            'es:keyword': True,
        }
    ])
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)