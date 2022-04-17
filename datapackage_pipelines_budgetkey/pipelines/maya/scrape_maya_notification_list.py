

from dataflows import Flow, printer, update_resource, set_primary_key,add_field


import logging

from time import sleep
import requests
import pytz
import os

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError


from dateutil.parser import parse
from datetime import date, datetime, time
from dateutil.relativedelta import relativedelta

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser import PARSER_VERSION

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def get_existing_records():
    try:
        engine = create_engine(get_connection_string())
        with engine.connect() as con:
            rs = con.execute(text("SELECT min(date), max(date) FROM maya_notifications where parser_version=:v"), v=PARSER_VERSION)
            row = rs.fetchone()
            return (row[0].date(), row[1].date()) if row[0] else (None, None)
    except (OperationalError, ProgrammingError) as e:
        #ProgrammingError is for postgres and OperationError is on sqlite
        logging.warning('Failed to fetch table maya_notifications')
    return (None, None)

def get_connection_string():
    connection_string = os.environ.get("DPP_DB_ENGINE", default=None)

    assert connection_string is not None, \
        "Couldn't connect to DB - " \
        "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
    return connection_string


def _get_maya_filter_request(date_from, date_to, page_num):
    events = [1400, 600, 1200, 1500, 900]
    sub_events = [  213, 221, 223, 224, 230, 238,
                    305, 306, 307, 308, 311, 314, 330,
                    601, 602, 603, 604, 605, 606,
                    611, 612, 613, 614, 615, 616,
                    620, 621, 622,
                    901, 902, 903, 904, 905, 906, 908, 909, 910, 911, 912,
                    1201,
                    1401, 1402, 1403, 1404, 1405, 1406,
                    1501, 1502, 1503, 1504]
    return {   "Page":page_num+1,
               "GroupData":[
                   {"DataList":[
                       {"Cd":evt,"Desc":"","IsSelected":True,"VFType":8} for evt in events
                   ]},
                   {"DataList":[
                       {"Cd":evt,"Desc":"","IsSelected":True,"VFType":7} for evt in sub_events]
                   }],
               "DateFrom":datetime.combine(date=date_from, time=time(0,0),tzinfo=pytz.utc).isoformat(),
               "DateTo":datetime.combine(date=date_to, time=time(0,0),tzinfo=pytz.utc).isoformat(),
               "IsBreakingAnnouncement":False,
               "IsForTaseMember":False,
               "IsSpecificFund":False,
               "IsTradeHalt":False,
               "QOpt":1,
               "ViewPage":2}


def _get_maya_headers():
    return {
        'X-Maya-With': "allow",
        'Content-Type': "application/json;charset=UTF-8",
        'Accept': "application/json, text/plain, */*",
        'Origin': "https://maya.tase.co.il",
        'Referer': "https://maya.tase.co.il",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
    }


def _maya_api_call(session, date_from, date_to, page_num):
    try:
        url = 'https://mayaapi.tase.co.il/api/report/filter'
        session.cookies.clear()
        res = session.post(url, json=_get_maya_filter_request(date_from, date_to, page_num), headers=_get_maya_headers())
        return res.json()
    except Exception as e:
        logging.error("Full Response code: {} body: {}".format(res.status_code, res.text));
        raise Exception("Failed to Call Maya API for date_from:{} date_to:{} page_num:{}".format(date_from, date_to, page_num)) from e

def _split_period(date_from, date_to):
    end_period = date_to
    start_period = end_period - relativedelta(months=6)
    while start_period > date_from:
        yield(start_period, end_period)
        end_period = start_period
        start_period = end_period - relativedelta(months=6)
    yield(date_from, end_period)


def _collect_date_range(session, date_from, date_to):

    current_page = 0
    has_more = True

    while has_more:
        res = _maya_api_call(session, date_from, date_to, current_page)
        sleep(3)

        # Figure out how many pages are available in total

        # Iterate over the available documents and get their individual URLs
        for report in res['Reports']:

            date_str = report['PubDate']
            #date_str will look like this 2018-05-21T23:27:32.687
            event_date = parse(date_str)

            # href is a string of the type reports/details/1162540 we are interested only in the id of the doc
            doc_id = report['RptCode']
            files = report['Files']

            segment_start = (doc_id//1000) * 1000
            segment_end = segment_start + 1000

            yield {
                'date': event_date,
                'source':'maya.tase.co.il',
                's3_object_name': 'maya.tase.co.il/{}/{}.htm'.format(event_date.strftime('%Y_%m'),doc_id),
                'url': 'https://mayafiles.tase.co.il/RHtm/{}-{}/H{}.htm'.format(segment_start+1, segment_end, doc_id),
                'pdf': [{
                    'name': f['Name'],
                    'url': 'https://mayafiles.tase.co.il/rpdf/{}-{}/P{}-{:02d}.pdf'.format(segment_start+1, segment_end, doc_id, i)
                } for i,f in enumerate(x for x in files if x['Type'] == 2) ],
                'other' : [f for f in files if f['Type'] != 2 and f['Name']],
                'num_files': len(files),
            }

        has_more = len(res['Reports']) > 0
        current_page += 1


def scrape_maya_notification_list():
    session = requests.Session()

    first_date, last_date = get_existing_records()
    if first_date:
        logging.info("Previously Scraped data From:{0:%Y-%m-%d} To:{1:%Y-%m-%d}".format(first_date, last_date))

        date_from = last_date - relativedelta(months=1)
        date_to = date.today()
        logging.info("Scrape New Maya data From:{0:%Y-%m-%d} To:{1:%Y-%m-%d}".format(date_from, date_to))
        for year_start, year_end in _split_period(date_from, date_to):
            yield from _collect_date_range(session, year_start, year_end)

        date_from = first_date - relativedelta(months=4)
        date_to = first_date + relativedelta(months=1)
        logging.info("Scrape Old Maya data From:{0:%Y-%m-%d} To:{1:%Y-%m-%d}".format(date_from, date_to))
        for year_start, year_end in _split_period(date_from, date_to):
            yield from _collect_date_range(session, year_start, year_end)

    else:
        date_from = date.today() - relativedelta(months=6)
        date_to = date.today()

        logging.info("Scrape Maya From:{0:%Y-%m-%d} To:{1:%Y-%m-%d}\n".format(date_from, date_to))
        for year_start, year_end in _split_period(date_from, date_to):
            yield from _collect_date_range(session, year_start, year_end)



def flow(*_):

    return Flow(
        add_field('date', 'date'),
        add_field('source', 'string'),
        add_field('s3_object_name', 'string'),
        add_field('url', 'string'),
        add_field('pdf', 'array'),
        add_field('other', 'array'),
        add_field('num_files', 'number'),
        set_primary_key(['s3_object_name']),
        scrape_maya_notification_list(),
        set_primary_key(['url']),
        update_resource(
            -1, name='maya_notification_list', path="data/maya_notification_list.csv",
            **{
                PROP_STREAMING: True,
            }
        ),
    )


if __name__ == '__main__':
    Flow(
        flow(),
        printer(),
    ).process()
