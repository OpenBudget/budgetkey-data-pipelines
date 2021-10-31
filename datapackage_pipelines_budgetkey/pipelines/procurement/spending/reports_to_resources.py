import hashlib
import os
import shutil
import tempfile
import urllib.parse
from itertools import chain
from copy import deepcopy
import datapackage
import functools
import requests
import tabulator
from tabulator.exceptions import SourceError
import logging
import time

from sqlalchemy import create_engine

from datapackage_pipelines_budgetkey.common.object_storage import temp_file, object_storage
from datapackage_pipelines.utilities.resources import PATH_PLACEHOLDER, PROP_STREAMING, PROP_STREAMED_FROM
from datapackage_pipelines.wrapper import ingest, spew, get_dependency_datapackage_url
from decimal import Decimal

from tableschema.exceptions import CastError

parameters, dp, res_iter = ingest()
input_file = get_dependency_datapackage_url(parameters['input-pipeline'])
db_table = parameters['db-table']
errors_db_table = parameters['error-db-table']

REVISION = 2

engine = create_engine(os.environ['DPP_DB_ENGINE'])
try:
    rp = engine.execute("""SELECT "report-url" from {} 
                        where "load-error" is not null
                        and "revision"={}""".format(errors_db_table, REVISION))
    errd_urls = set(r[0] for r in rp)
    rp = engine.execute("""SELECT distinct "report-url" from {}
                        where "revision"={}""".format(db_table, REVISION))
    all_good = set(r[0] for r in rp)
    logging.info('Got %d good reports, %d failed ones', len(all_good), len(errd_urls))
except:
    logging.exception('Failed to fetch report status')
    errd_urls = set()
    all_good = set()

order_id_headers = [
    'הזמנת רכש',
    'מספר הזמנה',
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
        'file://manual_fixes/ב 2016 מתוקן.xlsx',
    'https://foi.gov.il/sites/default/files/%D7%93%D7%95%D7%97%20%D7%94%D7%AA%D7%A7%D7%A9%D7%A8%D7%95%D7%99%D7%95%D7%AA%20%D7%9E%D7%A9%D7%A8%D7%93%20%D7%94%D7%AA%D7%97%D7%91%D7%95%D7%A8%D7%94%20%D7%95%D7%94%D7%A9%D7%9E%D7%98%20%D7%A8%D7%91%D7%A2%D7%95%D7%9F%20%D7%A9%D7%9C%D7%99%D7%A9%D7%99%202017%20%D7%9C%D7%A4%D7%A8%D7%A1%D7%95%D7%9D.xlsx':
	    'file://manual_fixes/2017-3-משרד התחבורה והבטיחות בדרכים-משרד התחבורה והבטיחות בדרכים-2017-11-14-f0fd.xlsx',
}

stats = {'bad-reports': 0}
loading_results = []
reports = datapackage.DataPackage(input_file).resources[0]
scraped = 0
try:
    for i, report in enumerate(reports.iter(keyed=True)):
        report_url = report['report-url']
        if report_url in all_good:
            # logging.info('SKIPPING ALL GOOD %s', report_url)
            continue
        if scraped > 1000 and report_url not in errd_urls:
            logging.info('SKIPPING DONE %s', report_url)
            continue
        logging.info('#%r: %r', i, report)
        report['revision'] = REVISION
        time.sleep(1)
        url_to_use = report_url
        if url_to_use in url_to_fixed_file:
            url_to_use = url_to_fixed_file[url_to_use]
            logging.info("Using fixed file: %s", url_to_use)
        if url_to_use.startswith('http'):
            hash = hashlib.md5(report['report-title'].encode('utf8')).hexdigest()[:4]
            obj_name = "{report-year}-{report-period}-{report-publisher}-{report-subunit}-{report-date}".format(**report)
            obj_name += '-' + hash
            _, ext = os.path.splitext(url_to_use)
            obj_name += ext
            obj_name = os.path.join('spending-reports', obj_name)
            if not object_storage.exists(obj_name):
                tmp = tempfile.NamedTemporaryFile()
                stream = None
                for _ in range(3):
                    try:
                        stream = requests.get(url_to_use, stream=True, verify=False).raw
                        break
                    except Exception as e:
                        logging.error('Failed to load data from %s; %s', url_to_use, e)
                        time.sleep(3)
                if stream is None:
                    logging.error('SKIPPING LOADING FILE %s', url_to_use)
                    continue
                stream.read = functools.partial(stream.read, decode_content=True)
                shutil.copyfileobj(stream, tmp)
                tmp.flush()
                url_to_use = object_storage.write(obj_name, file_name=tmp.name, create_bucket=False)
                tmp.close()
                del tmp
            else:
                url_to_use = object_storage.urlfor(obj_name)

        report['report-sheets'] = 0
        report['report-headers-row'] = None
        report['report-rows'] = None
        report['report-bad-rows'] = None
        report['load-error'] = None

        with tempfile.NamedTemporaryFile(suffix=os.path.splitext(url_to_use)[1]) as tmp:
            if url_to_use.startswith('http'):
                time.sleep(1)
                stream = requests.get(url_to_use, stream=True).raw
                stream.read = functools.partial(stream.read, decode_content=True)
                shutil.copyfileobj(stream, tmp)
                tmp.flush()
                filename = tmp.name
            else:
                filename = url_to_use

            good_sheets = 0
            errors = ''
            for sheet in range(1, 20):
                canary = None
                try:
                    try:
                        logging.info('Trying sheet %d in %s (using %s)', sheet, report['report-url'], url_to_use)
                        # logging.info('Temp filename %s', filename)
                        canary = tabulator.Stream(filename, sheet=sheet)
                        canary.open()
                    except SourceError as e:
                        logging.info("Detected %d sheets in %s", sheet-1, report['report-url'])
                        break
                    except Exception as e2:
                        logging.info('%s: Load file %s error, contents: %r',
                                     e2, filename, open(filename, 'rb').read(128))
                        raise
                    canary_rows = canary.iter()
                    headers = None
                    headers_row = None
                    for j, row in enumerate(canary_rows):
                        if j > 20:
                            break
                        row = [str(x).strip().replace('\xa0', ' ') if x is not None else '' for x in row]
                        if len(list(filter(lambda x: x, row))) < 2:
                            continue
                        row_fields = set(row)
                        if '' in row_fields:
                            row_fields.remove('')
                        if None in row_fields:
                            row_fields.remove(None)
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
                                assert all(sample_row[k].strip() in ['כן', 'לא', '', 'ערך היסטורי', None]
                                           for k in headers_row if k.startswith('הזמנה רגישה'))
                            except Exception as e:
                                logging.info('BAD SAMPLE ROW in %s:\n%r', report['report-url'], sample_row)
                                raise ValueError('Bad value for column (%s)' % e) from e
                    except StopIteration as e:
                        pass
                    good_sheets += 1
                    dp['resources'].append({
                        PROP_STREAMED_FROM: url_to_use,
                        'path': PATH_PLACEHOLDER,
                        'name': 'report_{}_{}'.format(i, sheet),
                        'headers': headers,
                        'sheet': sheet,
                        'constants': report
                    })
                    report['report-headers-row'] = headers
                    report['report-sheets'] = good_sheets
                except Exception as e:
                    logging.info("ERROR %r in %s", e, report['report-url'])
                    errors += str(e) + '\n'
                    continue
                finally:
                    if canary is not None:
                        canary.close()

            if good_sheets == 0:
                stats['bad-reports'] += 1
                report['report-sheets'] = 0
                report['report-headers-row'] = None
                report['load-error'] = errors.strip()
                report['report-rows'] = None
                report['report-bad-rows'] = None
            else:
                scraped += 1
                logging.info("Detected %d sheets in %s", good_sheets, report['report-url'])


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
    {
        'name': 'revision',
        'type': 'integer'
    },
])
dp['resources'].append({
    'path': 'data/report-loading-results.csv',
    PROP_STREAMING: True,
    'name': 'report-loading-results',
    'schema': schema
})

spew(dp, chain(res_iter, [loading_results]), stats)
