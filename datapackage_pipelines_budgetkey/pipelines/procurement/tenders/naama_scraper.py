import dataflows as DF
import datetime
import requests
from datapackage_pipelines_budgetkey.processors import data_gov_il_resource

tenders = {
    'dataset-name': 'tenders',
    'resource-name': 'דוח מכרזים'
}
exemptions = {
    'dataset-name': 'exemptions',
    'resource-name': 'דוח התקשרויות בפטור והליכים תחרותיים'
}


TENDER_MAPPING=dict(
    publication_id=['מספר פרסום'],
    tender_type_he=['סוג הליך'],
    tender_id=['מספר הליך'],
    description=['שם הליך'],
    subjects=['נושאים'],
    end_date=['תאריך סיום תקופת התקשרות'],
    start_date=['תאריך תחילת תקופת התקשרות'],
    status=['סטטוס'],
    publisher=['שם המשרד'],
    publisher_unit=['שם יחידה מפרסמת'],
    publication_date=['תאריך פרסום'],
    last_update_date=['תאריך עדכון'],
    claim_date=['תאריך אחרון להגשת השגות'],
    claim_time=['שעה אחרונה להגשת השגות'],
    supplier_id=['מספר חפ ספק'],
    supplier=['שם ספק זוכה'],

    regulation=['תקנה'],
    decision=['מהות החלטה'],

    approving_level=['גורם מאשר'],

    volume=['היקף כספי'],
    source_currency=['מטבע'],
)
TENDER_KINDS={
    'מכרז מרכזי': 'central',
    'מכרז פומבי': 'office',
    'פניה לקבלת מידע RFI': 'central',
    'קול קורא להקמת/עדכון רשימת מציעים (מאגר': 'central',
    
    'התקשרות בפטור במכרז או בהליך תחרותי אחר': 'exemptions',
    'פרסום כוונה להתקשרות': 'exemptions',
    'פרסום מיזם ללא כוונת רווח': 'exemptions',
    'פרסום עקרונות הסכם מסגרת עם  החברה  הממשלתית': 'exemptions',
    'פרסום התקשרות המשטרה עם בעל רעיון יחודי': 'exemptions',
}
KEY = ['publication_id', 'tender_type', 'tender_id']

def get_updated_sources():
    import requests
    from pyquery import PyQuery as pq
    URL = 'https://mr.gov.il/ilgstorefront/he/news/details/230920201036'
    sources = set()

    page = pq(requests.get(URL).text)
    anchors = page.find('a')
    for anchor in anchors:
        anchor = pq(anchor)
        href = anchor.attr('href')
        if '.zip' in href:
            sources.add(href + '#.xlsx')
    sources = [DF.load(source, format='excel-xml', encoding='utf8', bytes_sample_size=0, cast_strategy=DF.load.CAST_DO_NOTHING) for source in sources]
    if len(sources) != 2:
        return DF.Flow(
            data_gov_il_resource.flow(tenders),
            data_gov_il_resource.flow(exemptions),
        )
    else:
        return DF.Flow(*sources)



def flow(*_):
    return DF.Flow(
        get_updated_sources(),
        DF.concatenate(fields=TENDER_MAPPING, target=dict(name='tenders')),
        DF.filter_rows(lambda r: r['publication_id']),
        DF.add_field('tender_type', 'string', lambda r: TENDER_KINDS[r['tender_type_he']], **{'es:keyword': True}),
        DF.join_with_self('tenders', KEY, dict(
            (k, dict(aggregate='last'))
            for k in list(TENDER_MAPPING.keys()) + ['tender_type']
        )),

        DF.set_type('publication_id', type='string', transform=str),
        DF.set_type('supplier_id', type='string', transform=str),
        DF.set_type('tender_id', type='string', transform=lambda v: v or 'none'),
        DF.set_type('.+_date', type='date', format='%d.%m.%Y', on_error=DF.schema_validator.clear),
        DF.set_type('subjects', type='string', transform=lambda v: ';'.join(x.strip() for x in v.split(',')) if v else ''),
        DF.validate(),
        DF.set_type('claim_date', type='datetime', 
                    transform=lambda v, field_name, row: datetime.datetime.combine(v, row['claim_time'] or datetime.time(0)) if v else None),
        DF.set_type('tender_type_he', **{'es:keyword': True}),
        DF.delete_fields(['claim_time']),
        DF.add_field('page_url', 'string', lambda r: f'https://mr.gov.il/ilgstorefront/he/p/{r["publication_id"]}'),
        DF.add_field('page_title', 'string', lambda r: r['description']),
        DF.add_field('reason', 'string', lambda r: r['regulation']),
        DF.add_field('documents', 'array', []),
        DF.add_field('contact', 'string'),
        DF.add_field('contact_email', 'string'),
        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.printer(),
    )

if __name__ == '__main__':
    flow().process()