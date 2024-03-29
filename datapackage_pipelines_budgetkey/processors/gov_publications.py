import requests

from dataflows import (
    Flow, concatenate, set_type, update_resource, filter_rows,
    validate, delete_fields, add_field, set_primary_key,
    printer, ResourceWrapper
)
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver


URL = "https://www.gov.il/he/api/PublicationApi/Index?limit={limit}&OfficeId={office_id}&publicationType={publication_type}&skip={skip}"
# - office_id: "4fa63b79-3d73-4a66-b3f5-ff385dd31cc7"
# - publication_type: "7159e036-77d5-44f9-a1bf-4500e6125bf1" rfp
# - publication_type: "7b76d87f-d299-4019-8637-5f1de71c9523" supports


def fetcher(parameters):
    skip = 0
    gcd = None
    try:
        while True:
            url = URL.format(**parameters, limit=skip+10, skip=skip)
            skip += 10
            results = None
            if gcd is None:
                try:
                    results = requests.get(url)
                    results = results.json()
                except Exception as e:
                    print('FAILED to load from %s: %s' % (url, e))
                    if results and hasattr(results, 'content'):
                        print('FAILED to parse JSON <pre>%s</pre>' % results.content[:2048])
                    if gcd is None:
                        gcd = google_chrome_driver()
            if gcd is not None:
                results = gcd.json(url)
            results = results.get('results', [])
            yield from results
            if len(results) == 0:
                break
    finally:
        if gcd is not None:
            gcd.teardown()


def dedup():
    def func(rows: ResourceWrapper):
        pk = rows.res.descriptor['schema'].get('primaryKey', [])
        if len(pk) == 0:
            yield from rows
        else:
            keys = set()
            for row in rows:
                key = tuple(row[k] for k in pk)
                if key in keys:
                    continue
                keys.add(key)
                yield row
    return func


DATE_FMT = '%Y-%m-%dT%H:%M:%SZ'


def flow(parameters, *_):

    def take_first(field):
        def f(row):
            if field in row and isinstance(row[field], list):
                row[field] = row[field][0]
        return Flow(
            f, set_type(field, type='string'),
        )

    def datetime_to_date(field):
        def f(row):
            if row.get(field):
                row[field] = row[field].date()
        return Flow(
            f, set_type(field, type='date'),
        )

    def approve(parameters):
        def func(row):
            if parameters.get('filter-out') is None:
                return True
            bad_phrase = parameters['filter-out']
            for f in ('page_title', 'description'):
                if row.get(f) and bad_phrase in row[f]:
                    return False
            return True
        return func

    return Flow(
        fetcher(parameters),
        concatenate(dict(
            page_title=['Title'],
            publication_id=['ItemId'],
            tender_id=['ItemUniqueId'],
            publisher=['OfficeDesc'],
            start_date=['PublishDate'],
            claim_date=['LastDate'],
            decision=['StatusDesc'],
            description=['Description'],
            last_update_date=['UpdateDate'],
            base_url=['BaseUrl'],
            url_name=['UrlName'],
            tender_type_he=['PublicationTypeDesc'],
        ), resources=-1),
        add_field('tender_type', 'string', default=parameters['tender_type'], resources=-1),
        take_first('publisher'),
        take_first('tender_type_he'),
        add_field('page_url', 'string',
                  default=lambda row: 'https://www.gov.il/he{base_url}{url_name}'.format(**row)),
        # delete_fields(['base_url', 'url_name']),
        filter_rows(approve(parameters)),
        set_type('publication_id', type='integer'),
        set_type('start_date', type='datetime', format=DATE_FMT),
        set_type('last_update_date', type='datetime', format=DATE_FMT),
        set_type('claim_date', type='datetime', format=DATE_FMT),
        datetime_to_date('last_update_date'),
        datetime_to_date('start_date'),
        set_primary_key(['publication_id', 'tender_type', 'tender_id']),
        dedup(),
        update_resource(-1, **parameters.pop('resource')),
        update_resource(-1, **{'dpp:streaming': True}),
        validate(),
    )


if __name__ == '__main__':
    Flow(
        flow(dict(
            # office_id="4fa63b79-3d73-4a66-b3f5-ff385dd31cc7",
            tender_type='call_for_bids',
            office_id="",
            publication_type="7159e036-77d5-44f9-a1bf-4500e6125bf1",
            # publication_type="7b76d87f-d299-4019-8637-5f1de71c9523",
            resource=dict(name='publications')
        )), printer(), #lambda row: print(row)
    ).process()
