from itertools import chain
from copy import deepcopy
import datapackage
import tabulator
import logging

from datapackage_pipelines.wrapper import ingest, spew
from decimal import Decimal

from tableschema.exceptions import CastError

parameters, dp, res_iter = ingest()
input_file = parameters['input-file']

order_id_headers = [
    'הזמנת רכש',
    'מספר הזמנה'
]

budget_code_headers = [
    'תקנה תקציבית',
    'תקנה',
    'פריט התחייבות'
]

volume_headers = [
    'ערך ההזמנה כולל מע"מ',
    'ערך ההזמנה כולל מע"מ בש"ח',
    'ערך הזמנה במ.ט.מ'
]

url_to_fixed_file = {
    'https://foi.gov.il/sites/default/files/%D7%9E%D7%A9%D7%A8%D7%93%20%D7%94%D7%A8%D7%95%D7%95%D7%97%D7%94%20%D7%95%D7%94%D7%A9%D7%99%D7%A8%D7%95%D7%AA%D7%99%D7%9D%20%D7%94%D7%97%D7%91%D7%A8%D7%AA%D7%99%D7%99%D7%9D%20%D7%93%D7%95%D7%97%20%D7%94%D7%AA%D7%A7%D7%A9%D7%A8%D7%95%D7%99%D7%95%D7%AA%20%D7%9C%D7%A9%D7%A0%D7%AA%202016.xlsx':
        'file://manual_fixes/משרד הרווחה והשירותים החברתיים דוח התקשרויות לשנת 2016 מתוקן.xlsx',
    'https://foi.gov.il/sites/default/files/%D7%A2%D7%95%D7%AA%D7%A7%20%D7%A9%D7%9C%20%D7%A7%D7%95%D7%91%D7%A5%20%D7%A1%D7%95%D7%A4%D7%99%20%D7%94%D7%AA%D7%A7%D7%A9%D7%A8%D7%95%D7%99%D7%95%D7%AA%202015%20%D7%9E%D7%A0%D7%94%D7%9C%20%D7%94%D7%AA%D7%9B%D7%A0%D7%95%D7%9F%20%D7%A0%D7%95%D7%94%D7%9C%2010.xls':
        'file://manual_fixes/עותק של קובץ סופי התקשרויות 2015 מנהל התכנון נוהל 10 מתוקן.xls',
    'https://foi.gov.il/sites/default/files/2015_0.xlsx':
        'file://manual_fixes/2015_0 מתוקן.xlsx',
    'https://foi.gov.il/sites/default/files/%D7%90%27%202016.xlsx':
        'file://manual_fixes/א 2016 מתוקן.xlsx',
    'https://foi.gov.il/sites/default/files/%D7%91%27%202016.xlsx':
        'file://manual_fixes/ב 2016 מתוקן.xlsx'
}

stats = {'bad-reports': 0}
loading_results = []
reports = datapackage.DataPackage(input_file).resources[0]
try:
    for i, report in enumerate(reports.iter(keyed=True)):
        logging.info('#%r: %r', i, report)
        url_to_use = report['report-url']
        if url_to_use in url_to_fixed_file:
            url_to_use = url_to_fixed_file[url_to_use]
            logging.info("Using fixed file: %s", url_to_use)
        for sheet in range(1, 20):
            canary = None
            errd = True
            try:
                try:
                    logging.info('Trying sheet %d in %s', sheet, report['report-url'])
                    canary = tabulator.Stream(url_to_use, sheet=sheet)
                    canary.open()
                except IndexError as e:
                    logging.info("Detected %d sheets in %s", sheet-1, report['report-url'])
                    break
                canary_rows = canary.iter()
                headers = None
                for j, row in enumerate(canary_rows):
                    if j > 20:
                        break
                    row = [str(x).strip() if x is not None else '' for x in row]
                    if len(row) < 0 and row[0] == '':
                        continue
                    row_fields = set(row)
                    if '' in row_fields:
                        row_fields.remove('')
                    if len(row_fields) <= 4:
                        continue
                    if not set.intersection(row_fields, order_id_headers):
                        raise ValueError('Bad report format (order_id)')
                    if not set.intersection(row_fields, budget_code_headers):
                        raise ValueError('Bad report format (budget_code)')
                    if not set.intersection(row_fields, volume_headers):
                        raise ValueError('Bad report format (volume)')
                    headers = j+1
                    headers_row = row
                    break
                if headers is None:
                    raise ValueError('Failed to find headers')
                sample_row = None
                try:
                    for s in range(20):  # 20 sample rows
                        sample_row = next(canary_rows)
                        sample_row = dict(zip(headers_row, sample_row))
                        # Test some things make sense
                        try:
                            v = [Decimal(sample_row[k]) if sample_row[k] is not None else 0
                                 for k in headers_row
                                 if k.startswith('ביצוע') or k.startswith('ערך')]
                            assert all(sample_row[k] in ['כן', 'לא', '', 'ערך היסטורי', None]
                                       for k in headers_row if k.startswith('הזמנה רגישה'))
                        except Exception as e:
                            logging.info('BAD SAMPLE ROW in %s:\n%r', report['report-url'], sample_row)
                            raise ValueError('Bad value for column (%s)' % e) from e
                except StopIteration as e:
                    pass
                dp['resources'].append({
                    'url': url_to_use,
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
                    stats['bad-reports'] += 1
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
except CastError as e:
    for err in e:
        logging.error('Failed to cast value: %s', err)
    raise

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

spew(dp, chain(res_iter, [loading_results]), stats)
