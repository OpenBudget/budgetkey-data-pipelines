from itertools import chain
from copy import deepcopy
import datapackage
import tabulator
import logging

from datapackage_pipelines.wrapper import ingest, spew
from decimal import Decimal

parameters, dp, res_iter = ingest()
input_file = parameters['input-file']

loading_results = []
reports = datapackage.DataPackage(input_file).resources[0]
for i, report in enumerate(reports.iter()):
    for sheet in range(1, 20):
        canary = None
        errd = True
        try:
            try:
                logging.info('Trying sheet %d in %s', sheet, report['report-url'])
                canary = tabulator.Stream(report['report-url'], sheet=sheet)
                canary.open()
            except IndexError as e:
                logging.info("Detected %d sheets in %s", sheet-1, report['report-url'])
                break
            canary_rows = canary.iter()
            headers = None
            for j, row in enumerate(canary_rows):
                if j > 10:
                    break
                row = [str(x).strip() if x is not None else '' for x in row]
                if len(row) < 0 and row[0] == '':
                    continue
                row_fields = set(row)
                if '' in row_fields:
                    row_fields.remove('')
                if len(row_fields) <= 4:
                    continue
                if 'הזמנת רכש' not in row_fields:
                    raise ValueError('Bad report format (order_id)')
                if 'תקנה תקציבית' not in row_fields and 'תקנה' not in row_fields:
                    raise ValueError('Bad report format (budget_code)')
                headers = j+1
                headers_row = row
                break
            if headers is None:
                raise ValueError('Failed to find headers')
            sample_row = None
            for s in range(20):  # 20 sample rows
                sample_row = next(canary_rows)
                sample_row = dict(zip(headers_row, sample_row))
                # Test some things make sense
                try:
                    v = [Decimal(sample_row[k]) if sample_row[k] is not None else 0
                         for k in headers_row
                         if k.startswith('ביצוע') or k.startswith('ערך')]
                    assert all(sample_row[k] in ['כן', 'לא', '', 'ערך היסטורי']
                               for k in headers_row if k.startswith('הזמנה רגישה'))
                except Exception as e:
                    logging.info('BAD SAMPLE ROW in %s:\n%r', report['report-url'], sample_row)
                    raise ValueError('Bad value for column (%s)' % e) from e
            dp['resources'].append({
                'url': report['report-url'],
                'name': 'report_{}'.format(i),
                'headers': headers,
                'sheet': sheet,
                'constants': report
            })
            report['report-headers-row'] = headers
            report['report-sheets'] = sheet
            report['report-rows'] = None
            report['report-bad-rows'] = None
            report['load-error'] = None
            errd = False
        except Exception as e:
            if sheet == 1:
                logging.info("ERROR '%s' in %s", e, report['report-url'])
                report['report-sheets'] = 0
                report['report-headers-row'] = None
                report['load-error'] = str(e)
                report['report-rows'] = None
                report['report-bad-rows'] = None
            else:
                logging.info("Detected %d sheets in %s", sheet-1, report['report-url'])
        finally:
            if canary is not None:
                canary.close()
        if errd:
            break

    loading_results.append(report)

dp['resources'] = sorted(dp['resources'],
                         key=lambda res: '%s:%s' %
                                         (res['constants']['report-year'],
                                          res['constants']['report-period']))

schema = deepcopy(reports.descriptor['schema'])
schema['fields'].extend([
    {
        'name': 'report-headers-row',
        'type': 'integer'
    },
    {
        'name': 'report-sheets',
        'type': 'integer'
    },
    {
        'name': 'load-error',
        'type': 'string'
    },
    {
        'name': 'report-rows',
        'type': 'integer'
    },
    {
        'name': 'report-bad-rows',
        'type': 'integer'
    },
])
dp['resources'].append({
    'path': 'data/report-loading-results.csv',
    'name': 'report-loading-results',
    'schema': schema
})

spew(dp, chain(res_iter, [loading_results]))
