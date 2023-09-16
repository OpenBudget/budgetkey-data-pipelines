import datetime

from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    if spec['name'] == parameters['resource']:
        report_period = row['report-period']
        if not report_period:
            report_period = 4
        row['payment'] = {
            'year': row['report-year'],
            'period': row['report-period'],
            'date': str(row['report-date']) if row['report-date'] is not None else None,
            'title': row['report-title'],
            'url': row['report-url'],
            'timestamp': '{}-{}'.format(row['report-year'], report_period),
            'executed': float(row['executed']) if row['executed'] is not None else None,
            'volume': float(row['volume']) if row['volume'] is not None else None,
        }
        row['publisher_key'] = row['purchasing_unit'] or row['publisher'] or row['report-publisher']
    return row


def modify_datapackage(dp, parameters, stats):
    for resource in dp['resources']:
        if resource['name'] == parameters['resource']:
            resource['schema']['fields'].append({
                'name': 'payment',
                'type': 'object'
            })
            resource['schema']['fields'].append({
                'name': 'publisher_key',
                'type': 'string'
            })
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)