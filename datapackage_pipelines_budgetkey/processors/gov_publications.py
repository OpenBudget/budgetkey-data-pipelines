import requests

from dataflows import (
    Flow, concatenate, set_type, update_resource,
    validate, delete_fields, add_field, set_primary_key,
    printer
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
            results = requests.get(url)
            try:
                results = results.json()
            except Exception:
                print('FAILED to parse JSON <pre>%s</pre>' % results.content[:2048])
                if gcd is None:
                    gcd = google_chrome_driver()
                results = gcd.json(url)
            results = results.get('results', [])
            yield from results
            if len(results) == 0:
                break
    finally:
        if gcd is not None:
            gcd.teardown()


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
            if field in row:
                row[field] = row[field].date()
        return Flow(
            f, set_type(field, type='date'),
        )

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
        set_type('publication_id', type='integer'),
        set_type('start_date', type='datetime', format=DATE_FMT),
        set_type('last_update_date', type='datetime', format=DATE_FMT),
        set_type('claim_date', type='datetime', format=DATE_FMT),
        datetime_to_date('last_update_date'),
        datetime_to_date('start_date'),
        set_primary_key(['publication_id', 'tender_type', 'tender_id']),
        update_resource(-1, **parameters.pop('resource')),
        update_resource(-1, **{'dpp:streaming': True}),
        validate(),
        # printer(),
        # lambda rows: (row for row in rows if row['tender_id'].endswith('73f3')),
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
