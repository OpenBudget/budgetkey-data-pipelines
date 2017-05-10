import re
import datetime
import logging

from datapackage_pipelines.wrapper import process

DATE_RE = re.compile('[0-9]+')


def boolean(x):
    return isinstance(x, str) and x.strip() == 'כן'


def date(x):
    if isinstance(x, int) or isinstance(x, float):
        return datetime.date(1900, 1, 1) + datetime.timedelta(days=int(x))
    elif not isinstance(x, str):
        return x

    x = x.strip()
    if len(x) == 0:
        return None

    parts = DATE_RE.findall(x)
    if len(parts) >= 3:
        day = int(parts[0])
        month = int(parts[1])
        year = int(parts[2])
        if year < 100:
            year += 2000
        return datetime.date(year=year, month=month, day=day)


def budget_code(x):
    x = str(x).strip()
    if x.endswith('.0'):
        x = x[:-2]
    if len(x) == 11:
        x = '00'+x.replace('-', '').replace('=', '')
        assert(len(x) == 10)
    elif len(x) == 8:
        assert('-' not in x and '=' not in x)
        x = '00'+x
    elif len(x) == 7:
        assert('-' not in x and '=' not in x)
        x = '000'+x
    else:
        return None
    return x

BAD_WORDS = [
    'סה"כ',
    'סה״כ',
    '=SUBTOTAL',
    '=SUM'
]

bad_rows = {}
total_rows = {}

def process_row(row, row_index, spec, resource_index, *_):
    if resource_index == 0: # the data
        bad_rows.setdefault(row['report-url'], 0)
        total_rows.setdefault(row['report-url'], 0)

        total_rows[row['report-url']] += 1

        for v in row.values():
            for bw in BAD_WORDS:
                if isinstance(v, str) and bw in v:
                    bad_rows[row['report-url']] += 1
                    return

        try:
            for k, v in row.items():
                if k in ['sensitive_order']:
                    row[k] = boolean(v)
                elif k in ['budget_code']:
                    row[k] = budget_code(v)
                elif k in ['end_date', 'order_date', 'start_date']:
                    row[k] = date(v)
                elif k in ['volume']:
                    row[k] = float(v)
                elif k in ['executed']:
                    row[k] = float(v if v is not None else 0)
                elif isinstance(v, str):
                    row[k] = v.strip()
        except Exception as e:
            logging.error('ERROR in row %d: %r', row_index, row)
            bad_rows[row['report-url']] += 1
            return
    elif resource_index == 1: # the errors
        row['report-rows'] = total_rows.get(row['report-url'])
        row['report-bad-rows'] = bad_rows.get(row['report-url'])

    return row

process(process_row=process_row)
