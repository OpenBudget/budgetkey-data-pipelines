import logging
from time import sleep
import requests
import pytz
from dateutil.parser import parse
from datetime import date, datetime, time
from dateutil.relativedelta import relativedelta

from datapackage_pipelines.utilities.resources import PROP_STREAMING

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

session = requests.Session()


from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, _ = ingest()


def _get_maya_filter_request(date_from, date_to, page_num):
    events = [600]
    sub_events = [605,603,601,602,621,604,606,615,613,611,612,622,614,616]
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
               "QOpt":1,
               "ViewPage":2}


def _get_maya_headers():
    return {
        'X-Maya-With': "allow",
        'Content-Type': "application/json;charset=UTF-8",
        'Accept': "application/json, text/plain, */*",
    }


def _maya_api_call(date_from, date_to, page_num):
    try:
        url = 'https://mayaapi.tase.co.il/api/report/filter'
        session.cookies.clear()
        res = session.post(url, json=_get_maya_filter_request(date_from, date_to, page_num), headers=_get_maya_headers())
        return res.json()
    except Exception as e:
        raise Exception("Failed to Call Maya API for date_from:{} date_to:{} page_num:{}".format(date_from, date_to, page_num)) from e
    
def _split_period(date_from, date_to):
    current_date = date_from
    next = current_date + relativedelta(years=1)
    while next < date_to:
        yield(current_date, next)
        current_date = next
        next = current_date + relativedelta(years=1)
    yield(current_date, date_to)


def _collect_date_range(date_from, date_to):

    current_page = 0
    has_more = True

    while has_more:
        res = _maya_api_call(date_from, date_to, current_page)
        sleep(3)

        # Figure out how many pages are available in total

        # Iterate over the available documents and get their individual URLs
        for report in res['Reports']:

            date_str = report['PubDate']
            #date_str will look like this 2018-05-21T23:27:32.687
            date = parse(date_str)

            # href is a string of the type reports/details/1162540 we are interested only in the id of the doc
            doc_id = report['RptCode']

            segment_start = (doc_id//1000) * 1000
            segment_end = segment_start + 1000

            yield {
                'date': date,
                'source':'maya.tase.co.il',
                's3_object_name': 'maya.tase.co.il/{}/{}.htm'.format(date.strftime('%Y_%m'),doc_id),
                'url': 'https://mayafiles.tase.co.il/RHtm/{}-{}/H{}.htm'.format(segment_start+1, segment_end, doc_id)}

        has_more = len(res['Reports']) > 0
        current_page += 1


def collect():
    date_from = date(2011, 1, 1)
    if 'from' in parameters:
        date_from = datetime.strptime(parameters.get('from'), "%Y-%m-%d").date()

    date_to = date.today()
    if 'to' in parameters:
        date_to = datetime.strptime(parameters.get('to'), "%Y-%m-%d").date()

    for year_start, year_end in _split_period(date_from, date_to):
        yield from _collect_date_range(year_start, year_end)


datapackage['resources'].append({
    'path': 'data/notification-list.csv',
    'name': parameters.get('name'),
    PROP_STREAMING: True,
    'schema': {
        'fields': [
            {'name': 'url', 'type': 'string'},
            {'name': 's3_object_name', 'type': 'string'},
            {'name': 'source', 'type':'string'},
            {'name': 'date', 'type': 'datetime'}
        ]
    }
})

spew(datapackage, [collect()])


