import math

import logging
from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, *_):
    return dp


def process_row(row, *_):
    purpose = row.get('purpose')
    publisher_name = row.get('publisher_name')
    if purpose:
        if publisher_name == 'שירות בתי הסוהר':
            purpose = purpose.split('איש קשר')[0]
            row['purpose'] = purpose
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)