from dataflows import (
    Flow, load, concatenate, update_resource,
    set_primary_key, set_type, printer
)
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver



# Map the original column headers (in Hebrew) to the new column names (in English)
COLUMN_HEADERS_MAPPER = {
    'שם חברה': 'company_name',
    'מספר חברה':'id',
    'שם באנגלית':'company_name_eng',
    'סוג תאגיד':'company_type',
    'סטטוס חברה':'company_status',
    'תאור חברה':'company_description',
    'מטרת החברה':'company_goal',
    'תאריך התאגדות':'company_registration_date',
    'חברה ממשלתית':'company_is_government',
    'מגבלות':'company_limit',
    'מפרה':'company_is_mafera',
    'שנה אחרונה של דוח שנתי (שהוגש)':'company_last_report_year',
    'שם עיר':'company_city',
    'מיקוד':'company_postal_code',
    'ת.ד.':'company_pob',
    'מדינה':'company_country',
    'אצל':'company_located_at',
}



def _get_columns_mapping_dict():
    """
    Prepares the dict object for the concatenate function, which 'creates' new column names for existing column data

    Returns:
        dict: A dict of format {'new_column_header': ['original_column_header]},
    """

    columns_mapping_dict = {}
    for original_header in COLUMN_HEADERS_MAPPER:
        new_header = COLUMN_HEADERS_MAPPER[original_header]
        columns_mapping_dict[new_header] = [original_header]
    return columns_mapping_dict

def clear_bool_values(package):
    for res in package.pkg.descriptor['resources']:
        for field in res['schema']['fields']:
            if 'falseValues' in field:
                del field['falseValues']
            if 'trueValues' in field:
                del field['trueValues']
    yield package.pkg
    yield from package


def flow(*_):
    gcd = google_chrome_driver()
    download = gcd.download('https://data.gov.il/dataset/246d949c-a253-4811-8a11-41a137d3d613/resource/f004176c-b85f-4542-8901-7b3176f9a054/download/f004176c-b85f-4542-8901-7b3176f9a054.csv')
    return Flow(
        load(download, cast_strategy=load.CAST_TO_STRINGS),
        concatenate(_get_columns_mapping_dict(), target=dict(name='company-details')),
        set_type('id', type='string'),
        set_type('company_registration_date', type='date', format='%d/%m/%Y'),
        set_type('company_is_government', type='boolean', falseValues=['לא'], trueValues=['כן']),
        set_type('company_is_mafera', type='boolean', falseValues=['לא'], trueValues=['מפרה', 'התראה']),
        set_type('company_last_report_year', type='integer'),
        clear_bool_values,
        update_resource(**{'dpp:streaming': True}, resources='company-details'),
        set_primary_key(['id'], resources='company-details'),
        printer(),
    )


if __name__ == '__main__':
    flow().process()
