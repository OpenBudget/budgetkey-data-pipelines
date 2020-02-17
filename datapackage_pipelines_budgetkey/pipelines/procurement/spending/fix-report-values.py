import re
import datetime
import logging
from decimal import Decimal

from datapackage_pipelines.wrapper import process

DATE_RE = re.compile('[0-9]+')
ORDER_ID_RE = re.compile('[0-9]+')

order_id_counter = 0


def order_id(x):
    global order_id_counter
    if ORDER_ID_RE.fullmatch(x):
        return x
    order_id_counter += 1
    return x+'-%04x' % order_id_counter


def boolean(x):
    return isinstance(x, str) and x.strip() == 'כן'


def date(x):
    if x is None:
        return None

    try:
        x = float(x)
        return datetime.date(1900, 1, 1) + datetime.timedelta(days=int(x))
    except ValueError:
        pass

    if not isinstance(x, str):
        return x

    x = x.strip()
    if len(x) == 0:
        return None

    parts = DATE_RE.findall(x)
    if len(parts) == 3:
        day = int(parts[0])
        month = int(parts[1])
        year = int(parts[2])
    elif len(parts) == 6:
        year = int(parts[0])
        month = int(parts[1])
        day = int(parts[2])
    else:
        assert False

    if year < 100:
        year += 2000
    return datetime.date(year=year, month=month, day=day)


def budget_code(x):
    assert x is not None, 'Budget code is None'

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
        assert False, 'Bad budget code %s' % x
    return x

BAD_WORDS = [
    'סה"כ',
    'סה״כ',
    'הזמנת רכש רגישה',
    '=SUBTOTAL',
    '=SUM'
]

bad_rows = {}
total_rows = {}
last_error_url = None


def process_row(row, row_index, spec, resource_index, parameters, stats):
    if resource_index == 0:  # the data
        if row_index == 0:
            stats['bad-lines'] = 0
            stats['good-lines'] = 0

        bad_rows.setdefault(row['report-url'], 0)
        total_rows.setdefault(row['report-url'], 0)

        total_rows[row['report-url']] += 1

        non_empty = 0
        for v in row.values():
            if v:
                non_empty += 1
            for bw in BAD_WORDS:
                if isinstance(v, str) and bw in v:
                    bad_rows[row['report-url']] += 1
                    return

        if non_empty <= 8:
            return

        try:
            assert row['order_id']
            for k, v in row.items():
                if k in ['order_id']:
                    row[k] = order_id(v)
                elif k in ['sensitive_order']:
                    row[k] = boolean(v)
                elif k in ['budget_code']:
                    row[k] = budget_code(v)
                elif k in ['end_date', 'order_date', 'start_date']:
                    row[k] = date(v)
                elif k in ['currency']:
                    if not v: v = 'ILS'
                    row[k] = v.upper() if isinstance(v, str) else v
                elif k in ['volume']:
                    row[k] = Decimal(v.replace(',', '') if v is not None and v != '' else 0)
                elif k in ['executed']:
                    row[k] = Decimal(v.replace(',', '') if v is not None and v != '' else 0)
                elif isinstance(v, str):
                    row[k] = v.strip()
            stats['good-lines'] += 1
        except Exception:
            stats['bad-lines'] += 1
            global last_error_url
            if last_error_url != row['report-url']:
                logging.exception('ERROR #%d in row %d: %r', stats['bad-lines'], row_index, row)
                last_error_url = row['report-url']
            bad_rows[row['report-url']] += 1
            return
    elif resource_index == 1: # the errors
        row['report-rows'] = total_rows.get(row['report-url'])
        row['report-bad-rows'] = bad_rows.get(row['report-url'])

    return row

process(process_row=process_row)
