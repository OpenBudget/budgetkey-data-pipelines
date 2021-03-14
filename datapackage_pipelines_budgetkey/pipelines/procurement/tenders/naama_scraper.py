import dataflows as DF
import datetime
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
    
    'התקשרות בפטור במכרז או בהליך תחרותי אחר': 'exemption',
    'פרסום כוונה להתקשרות': 'exemption',
    'פרסום מיזם ללא כוונת רווח': 'exemption',
    'פרסום עקרונות הסכם מסגרת עם  החברה  הממשלתית': 'exemption',
    'פרסום התקשרות המשטרה עם בעל רעיון יחודי': 'exemption',
}



def flow(*_):
    return DF.Flow(
        data_gov_il_resource.flow(tenders),
        data_gov_il_resource.flow(exemptions),
        DF.concatenate(fields=TENDER_MAPPING),
        DF.validate(),
        DF.filter_rows(lambda r: r['publication_id']),
        DF.set_type('supplier_id', type='string', transform=str),
        DF.set_type('tender_id', type='string', transform=lambda v: v or 'none'),
        DF.set_type('.+_date', type='date', format='%d.%m.%Y', on_error=DF.schema_validator.clear),
        DF.set_type('subjects', type='array', transform=lambda v: list(x.strip() for x in v.split(',')) if v else []),
        DF.set_type('claim_date', type='datetime', 
                    transform=lambda v, field_name, row: datetime.datetime.combine(v, row['claim_time'] or datetime.time(0)) if v else None),
        DF.set_type('tender_type_he', **{'es:keyword': True}),
        DF.delete_fields(['claim_time']),
        DF.add_field('tender_type', 'string', lambda r: TENDER_KINDS[r['tender_type_he']], **{'es:keyword': True}),
        DF.add_field('page_url', 'string', lambda r: f'https://mr.gov.il/ilgstorefront/he/p/{r["publication_id"]}'),
        DF.add_field('page_title', 'string', lambda r: r['description']),
        DF.add_field('reason', 'string', lambda r: r['regulation']),
        DF.add_field('documents', 'array', []),
        DF.add_field('contact', 'array', []),
        DF.add_field('contact_email', 'array', []),
        DF.validate(),
        DF.update_resource(-1, name='tenders', **{'dpp:streaming': True}),
        DF.printer(),
    )

if __name__ == '__main__':
    flow().process()