import datetime

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines_budgetkey.common.guidestar_api import GuidestarAPI


def lineSplitter(x):
    return [xx.strip() for xx in x.split('\n')]


def pickFirst(x):
    return x[0] if len(x)>0 else None


def commaJoiner(x):
    return ','.join(x)


def boolToCred(x):
    return 'יש אישור' if x else 'אין אישור'

def newlineRemover(x):
    return x.replace('\n', '')

def getter(out_field, path, processors, options={'type': 'string'}):
    out_field = 'association_' + out_field
    def _f(data, out):
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
    getter('num_of_employees', 'employeesNum', [],
            dict(type='number')),
    getter('num_of_volunteers', 'volunteersNum', [],
            dict(type='number')),
    getter('yearly_turnover', 'turnover', [],
            dict(type='number')),
    getter('website', 'website', []),
    getter('email', 'email', []),
    getter('objective', 'orgGoal', [newlineRemover]),
    getter('year_established', 'orgYearFounded', [],
            dict(type='integer')),
    getter('org_kind', 'malkarType', []),
    getter('proper_management', 'hasNihulTakin', [boolToCred]),
    getter('guidestar_title', 'name', []),
    getter('address', 'fullAddress', []),
    getter('address_city', 'addressCity', []),

    getter('ceo', 'ceoName', []),
    getter('last_report_year', 'lastFinancialReportYear', [int],
            dict(type='integer')),
    getter('field_of_activity', 'secondaryClassifications', [pickFirst]),
    getter('fields_of_activity', 'secondaryClassifications', [],
            {'type':'array', 'es:itemType': 'string'}),
    getter('primary_field_of_activity', 'primaryClassifications', [pickFirst]),
    getter('org_status', 'malkarStatus', []),
    getter('activity_region_list', 'activityAreas', [],
            {'type':'array', 'es:itemType': 'string'}),
    getter('activity_region', 'activityAreas', [commaJoiner]),
    getter('has_article_46', 'approval46', [boolToCred])
]

def scrape_guidestar(ass_recs, diluter=None):

    api = GuidestarAPI()

    for i, ass_rec in enumerate(ass_recs):

        if not ass_rec['__is_stale']:
            continue

        assert 'Association_Number' in ass_rec
        anum = ass_rec['Association_Number']

        data = api.organization(anum)

        if data is None:
            continue

        rec = {}
        for _, _, rule in rules:
            rule(data, rec)

        rec['id'] = ass_rec['Association_Number']
        rec['association_registration_date'] = ass_rec['Association_Registration_Date']
        rec['association_title'] = ass_rec['Association_Name']
        rec['association_address_lines'] = [ rec['association_address'] ]

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
