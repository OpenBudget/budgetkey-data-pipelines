import re
import requests
import time
import logging
import datetime

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines_budgetkey.common.guidestar_api import GuidestarApi


def lineSplitter(x):
    return [xx.strip() for xx in x.split('\n')]


def pickFirst(x):
    return x[0] if len(x)>0 else None


def commaJoiner(x):
    return ','.join(x)


def boolToCred(x):
    return 'יש אישור' if x else 'אין אישור'


def timestampParser(x):
    return datetime.datetime.fromtimestamp(x/1000)


def timestampYearParser(x):
    return 1 if x else 0

def newlineRemover(x):
    return x.replace('\n', '')

def salariesExtractor(x):
    return [
        dict(
            role=xx['MainLabel'],
            salary=xx['Amount']
        )
        for xx in x
    ]


def getter(out_field, entry, path, processors, options={'type': 'string'}):
    out_field = 'association_' + out_field
    def _f(data, out):
        data = data[entry]['result']
        parts = path.split('.')
        for p in parts:
            if isinstance(data, list):
                try:
                    p = int(p)
                    if p < len(data):
                        data = data[p]
                    else:
                        data = None
                        break
                except:
                    data = None
                    break
            else:
                data = data.get(p)
            # logging.error('%s %s %s %s' % (path, p, data, '-->'))
            if data is None:
                break
        for p in processors:
            if data is not None:
                data = p(data)
            # logging.error('%s %s' % (out_field, data))
        if options['type'] == 'array' and data is None:
            data = []
        out[out_field] = data
    return out_field, options, _f


rules = [
    getter('founders', 0, 'result.founderNames', [lineSplitter], 
            {'type':'array', 'es:itemType': 'string'}),
    getter('year_established', 0, 'result.orgYearFounded', [],
            dict(type='integer')),

    getter('guidestar_title', 0, 'result.Name', []),
    getter('org_status', 0, 'result.isStatusActiveText', []),
    getter('org_kind', 0, 'result.sugHitagdut', []),

    getter('address_house_num', 0, 'result.addressHouseNum', []),
    getter('address_street', 0, 'result.addressStreet', []),
    getter('address_zip_code', 0, 'result.addressZipCode', []),
    getter('address_city', 0, 'result.city', []),

    getter('activity_region_list', 0, 'result.cities', [],
            {'type':'array', 'es:itemType': 'string'}),
    getter('activity_region', 0, 'result.cities', [commaJoiner]),
    getter('activity_region_national', 0, 'result.malkarLocationIsNational', [],
            dict(type='boolean')),

    getter('email', 0, 'result.greenInfo.email', []),
    getter('facebook', 0, 'result.greenInfo.facebookUrl', []),
    getter('website', 0, 'result.greenInfo.websiteUrl', []),
    getter('logo_url', 0, 'result.logoUrl', []),

    getter('proper_management', 0, 'result.hasProperManagement', [boolToCred]),
    getter('has_article_46', 0, 'result.approval46', [boolToCred]),

    getter('field_of_activity', 0, 'result.tchumPeilutSecondary', [pickFirst]),
    getter('fields_of_activity', 0, 'result.tchumPeilutSecondary', [],
            {'type':'array', 'es:itemType': 'string'}),
    getter('primary_field_of_activity', 0, 'result.tchumPeilutMain', []),

    getter('objective', 0, 'result.orgGoal', [newlineRemover]),

    getter('yearly_turnover', 0, 'result.turnover', [],
            dict(type='number')),
    getter('num_of_employees', 0, 'result.employees', [],
            dict(type='number')),
    getter('num_of_volunteers', 0, 'result.volunteers', [],
            dict(type='number')),

    getter('top_salaries', 1, 'result.0.Data', [salariesExtractor],
            {'type':'array', 'es:itemType': 'object', 'es:index': False}),

    getter('last_report_year', 0, 'result.lastAddDataYear', [int],
            dict(type='integer')),
    getter('online_data_update_year', 0, 'result.InactiveMenu.people', [timestampYearParser],
            dict(type='integer')),
]
# TODO:
# - address_lines

def scrape_guidestar(ass_recs, diluter=None):

    for i, ass_rec in enumerate(ass_recs):

        if not ass_rec['__is_stale']:
            continue

        if diluter is not None and i % 13 != diluter:
            continue

        assert 'Association_Number' in ass_rec
        anum = ass_rec['Association_Number']

        api = GuidestarApi('https://www.guidestar.org.il/he/organization/{}'.format(anum))

        data = api.prepare()\
            .method('getMalkarDetails', [anum], 39)\
            .method('getMalkarWageEarners', [anum], 39)\
            .method('getMalkarDonations', [anum], 39)\
            .run()

        if data is None:
            continue

        rec = {}
        for _, _, rule in rules:
            rule(data, rec)

        rec['id'] = ass_rec['Association_Number']
        rec['association_registration_date'] = ass_rec['Association_Registration_Date']
        rec['association_title'] = ass_rec['Association_Name']

        address = ''
        if rec.get('association_address_street'):
            address += rec['association_address_street']
        if rec.get('association_address_house_num'):
            address += ' ' + rec['association_address_house_num']
        if rec.get('association_address_city'):
            address += ', ' + rec['association_address_city']
        if rec.get('association_address_zip_code'):
            address += ' ' + rec['association_address_zip_code']
        rec['association_address_lines'] = [ address ]
        rec['association_address'] = ' '.join(rec['association_address_lines'])

        yield rec


def process_resources(res_iter_, dilute=False):
    first = next(res_iter_)
    yield scrape_guidestar(first, datetime.date.today().isocalendar()[1] % 13 if dilute else None)

    for res in res_iter_:
        yield res


if __name__ == '__main__':
    parameters, datapackage, res_iter = ingest()

    dilute = parameters.pop('dilute', False)

    resource = datapackage['resources'][0]
    resource.update(parameters)
    resource.setdefault('schema', {})['fields'] = [
        dict(name=field, **options)
        for field, options, _ in sorted(rules, key=lambda r:r[0])
    ]
    resource['schema']['fields'].extend([
        {
            'name': 'association_address',
            'type': 'string'
        },
        {
               'name': 'association_address_lines',
            'type': 'array',
            'es:itemType': 'string'
        },
        {
            'name': 'id',
            'type': 'string'
        },
        {
            'name': 'association_title',
            'type': 'string'
        },
        {
            'name': 'association_registration_date',
            'type': 'date'
        }
    ])
    resource['schema']['missingValues'] = ['', "לא קיים דיווח מקוון"]

    spew(datapackage, process_resources(res_iter, dilute))
