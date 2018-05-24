import re
import requests
import time
import logging
import datetime

from datapackage_pipelines.wrapper import ingest, spew


CSRF_RE = re.compile('\{"name":"getUserInfo","len":0,"ns":"","ver":39.0,"csrf":"([^"]+)"\}')
VID_RE = re.compile('RemotingProviderImpl\(\{"vf":\{"vid":"([^"]+)"')


HEADERS = {
    'X-User-Agent': 'Visualforce-Remoting',
    'Origin': 'https://www.guidestar.org.il',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,he;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
    'Content-Type': 'application/json',
    'Accept': '*/*',
    'Referer': 'https://www.guidestar.org.il/organization/580030104',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
}

def lineSplitter(x):
    return [xx.strip() for xx in x.split('\n')]


def commaJoiner(x):
    return ','.join(x)


def boolToCred(x):
    return 'יש אישור' if x else 'אין אישור'


def timestampParser(x):
    return datetime.datetime.fromtimestamp(x/1000)


def timestampYearParser(x):
    return timestampParser(x).year

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

    getter('field_of_activity', 0, 'result.tchumPeilutSecondary', []),
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
    getter('online_data_update_year', 0, 'result.lastModifiedDate', [timestampYearParser],
            dict(type='integer')),
]
# TODO:
# - address_lines

def scrape_guidestar(ass_recs):
    for i, ass_rec in enumerate(ass_recs):

        if not ass_rec['__is_stale']:
            continue

        assert 'Association_Number' in ass_rec
        anum = ass_rec['Association_Number']

        guidestar_url = 'https://www.guidestar.org.il/he/organization/{}'.format(anum)
        page = None
        while page is None:
            try:
                response = requests.get(guidestar_url)
                if response.status_code == 200:
                    page = response.text
            except:
                time.sleep(60)
        
            if page is None:
               continue

        csrf = CSRF_RE.findall(page)[0]
        vid = VID_RE.findall(page)[0]
        
        body = []
        for i, method in enumerate(["getMalkarDetails", 
                                    "getMalkarWageEarners",
                                    "getMalkarDonations"]):
            body.append({
                "action": "GSTAR_Ctrl",
                "method": method,
                "data": [anum],
                "type": "rpc",
                "tid": 3 + i,
                "ctx": {
                    "csrf": csrf,
                    "ns":"",
                    "vid": vid,
                    "ver":39
                }
            })

        data = None
        for i in range(5):
            try:
                data = requests.post('https://www.guidestar.org.il/apexremote', 
                json=body, headers=HEADERS).json()
                break
            except Exception as e:
                logging.exception('Failed to fetch data for %s: %r', anum, e)
                time.sleep(60)
        if data is None:
            continue
        for entry in data:
            if 'result' not in entry:
                data = None
                break            
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


def process_resources(res_iter_):
    first = next(res_iter_)
    yield scrape_guidestar(first)

    for res in res_iter_:
        yield res


if __name__ == '__main__':
    parameters, datapackage, res_iter = ingest()

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

    spew(datapackage, process_resources(res_iter))
