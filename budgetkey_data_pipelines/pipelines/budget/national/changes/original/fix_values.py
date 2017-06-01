import datetime

from datapackage_pipelines.wrapper import process

formats = [
    '%d/%m/%Y',
    '%Y-%m-%dT%H:%M:%S',
    '%m/%d/%y',
]


def process_row(row, *_):
    for k in list(row.keys()):
        if k.startswith('date/'):
            v = row[k]
            if v is None or v.strip() == '':
                row[k] = None
                continue
            for fmt in formats:
                try:
                    d = datetime.datetime.strptime(v, fmt).date()
                    row[k] = d
                except ValueError:
                    continue

    return row


process(process_row=process_row)
