from datapackage_pipelines.wrapper import ingest, spew
from functools import partial
from datetime import date
from dataflows.helpers.extended_json import DATE_F_FORMAT

parameters, dp, res_iter = ingest()


def get_no_date_range(row):
    return '1900-01-01', '2100-01-01', []


def get_year_date_range(field_min, field_max, row):
    year_min = row[field_min]
    if isinstance(year_min, date):
        year_min = year_min.year
    if field_max:
        year_max = row[field_max]
        if isinstance(year_max, date):
            year_max = year_max.year
    else:
        year_max = year_min
    return '{:04d}-01-01'.format(year_min), '{:04d}-12-31'.format(year_max), ["{:04d}-{:0>2}".format(year, i) for year in range(year_min, year_max+1) for i in range(1,13)]


def get_date_range(from_field, to_field, row):
    if not row[from_field] and not row[to_field]:
        return get_no_date_range(row)
    elif not row[from_field]:
        return row[to_field].strftime(DATE_F_FORMAT), row[to_field].strftime(DATE_F_FORMAT), [row[to_field].strftime('%Y-%m')]
    elif not row[to_field]:
        return row[from_field].strftime(DATE_F_FORMAT), row[from_field].strftime(DATE_F_FORMAT), [row[from_field].strftime('%Y-%m')]
    else:
        months = []
        for year in range(row[from_field].year, row[to_field].year+1):
            from_month = row[from_field].month if year == row[from_field].year else 1
            to_month = row[to_field].month if year == row[to_field].year else 12
            for month in range(from_month, to_month+1):
                months.append('{}-{:0>2}'.format(year, month))
        return row[from_field].strftime(DATE_F_FORMAT), row[to_field].strftime(DATE_F_FORMAT), months


get_date_range_func = {
    'no-date-range': get_no_date_range,
    'year': partial(get_year_date_range, parameters.get('field'), None),
    'year-range': partial(get_year_date_range, parameters.get('from-field'), parameters.get('to-field')),
    'date-range': partial(get_date_range, parameters.get('from-field'), parameters.get('to-field')),
}[parameters.get('type', 'no-date-range')]


def process_resource(res):
    for row in res:
        row['__date_range_from'], row['__date_range_to'], row['__date_range_months'] = get_date_range_func(row)
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


dp['resources'][0]['schema']['fields'] += [
    {'name': '__date_range_from', 'type': 'date'},
    {'name': '__date_range_to', 'type': 'date'},
    {'name': '__date_range_months', 'type': 'array', 'es:itemType': 'string', 'es:keyword': True},
]


spew(dp, process_resources(res_iter))
