# coding=utf-8
from dataflows import Flow, printer, delete_fields, load
import csv
from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser_utils import verify_row_count, \
    add_fields, rename_fields, verify_row_values

SUG_MISPAR_ZIHUY_MAPPING = {'מספר ת.ז.': 'id_number', 'מספר דרכון': 'passport_number', 'מספר זיהוי לא ידוע': 'unknown'}

EMPTY_STRING = '_________'

FIELD_CONVERSION = {
    'IsIsraeli': (lambda val: 'אדם פרטי עם אזרחות ישראלית' in val),
    'IsBothDirectorAndCeoOrRelativeOfCeo': (lambda val: True if 'כן' == val else (False if 'לא' == val else None) ),
    'EmployedAtAnotherJobConnectedToTheCompany': (lambda val: 'ממלא' == val) ,
    'RelativeOfAnotherVip': (lambda val: 'הינו' == val),
    'HasStocksInTheCompany': (lambda val: 'מחזיק' == val) ,
    'HasStocksInSubsidiaryOrConnectedCompany': (lambda val: True if 'מחזיק' == val else (False if 'אינו מחזיק' == val else None) )
}

RENAME_FIELDS = {
    'Shem': 'FullName',
    'ShemPriatiVeMishpacha': 'FullName',
    'EnglishName': 'FullNameEn',
    'ShemYeshutIvrit': 'CompanyName',
    'ShemYeshutKodem': 'PreviousCompanyNames',
    'ShemYeshutLoazit': 'CompanyNameEn',
    'EretzEzrachut': 'Citizenship',
    'Ezrachut': 'IsIsraeli',
    'TaarichTchilatHaKehuna': 'AppointmentDate',
    'TaarichTchilatHaCehuna': 'AppointmentDate',
    'ApprovalDate': 'AppointmentApprovalDate',
    'GeneralMeeting': 'AppointmentApprovalDate',
    'IsDirectorAndCEO': 'IsBothDirectorAndCeoOrRelativeOfCeo',
    'TextHofshi': 'Comments',
    'NoseMisraBechira1': 'EmployedAtAnotherJobConnectedToTheCompany',
    'Hesber1': 'EmployedAtAnotherJobConnectedToTheCompanyExplanation',
    'NoseMisraBechira2': 'RelativeOfAnotherVip',
    'Hesber2': 'RelativeOfVipExplanation',
    'NoseMisraBechira3': 'HasStocksInTheCompany',
    'NoseMisraBechira4': 'HasStocksInSubsidiaryOrConnectedCompany',

    'MisparZihuy1': 'MisparZihuy',
    'SugMisparZihuy1':'SugMisparZihuy',
}

FIELDS = [
    'SugMisparZihuy',
    'MisparZihuy',
    'FullName',
    'FullNameEn',
    'CompanyName',
    'PreviousCompanyNames',
    'CompanyNameEn',
    'Citizenship',
    'IsIsraeli',
    'AppointmentDate',
    'AppointmentApprovalDate',
    'IsBothDirectorAndCeoOrRelativeOfCeo',
    'Comments',
    'EmployedAtAnotherJobConnectedToTheCompany',
    'EmployedAtAnotherJobConnectedToTheCompanyExplanation',
    'RelativeOfAnotherVip',
    'RelativeOfVipExplanation',
    'HasStocksInTheCompany',
    'HasStocksInSubsidiaryOrConnectedCompany',
    'HeaderMisparBaRasham',
    'HeaderSemelBursa',
    'KodSugYeshut',
    'MezahehHotem',
    'MezahehTofes',
    'NeyarotErechReshumim',
    'PumbiLoPumbi']

def validate(rows):
    for row in rows:
        verify_row_count(row, FIELDS, 1)
        verify_row_values(row, 'IsIsraeli', {'אדם פרטי ללא אזרחות ישראלית', 'אדם פרטי עם אזרחות ישראלית'})
        verify_row_values(row, 'IsBothDirectorAndCeoOrRelativeOfCeo', {'_________', 'כן', 'לא'})
        verify_row_values(row, 'EmployedAtAnotherJobConnectedToTheCompany', {'אינו ממלא', 'ממלא'})
        verify_row_values(row, 'RelativeOfAnotherVip', {'אינו', 'הינו'})
        verify_row_values(row, 'HasStocksInTheCompany', {'אינו מחזיק', 'מחזיק'})
        verify_row_values(row, 'HasStocksInSubsidiaryOrConnectedCompany', {'_________', 'אינו מחזיק', 'מחזיק'})
        yield row


def filter_by_type(rows):
    for row in rows:
        if row['type'] == 'ת091':
            yield row


def _is_null_string(string):
    return not string or string == '-' * len(string) or string == '_' * len(string)


def parse_document(rows):
    for row in rows:
        doc = row['document']
        previous_jobs_at_the_company = []
        for title1, title2 in zip(doc.get('TafkidKodem', []), doc.get('TafkidKodemAher', [])):
            if title1 == 'אין':
                continue
            elif title1 == 'אחר':
                previous_jobs_at_the_company.append(title2)
            elif _is_null_string(title2):
                previous_jobs_at_the_company.append(title1)
            else:
                previous_jobs_at_the_company.append('{} {}'.format(title1, title2))

        job_titles = []
        for title1, title2 in zip(doc.get('HaTafkidLoMuna', []), doc.get('TeurHaTafkitLoMuna', [])):
            if title1 == 'אחר':
                job_titles.append(title2)
            elif _is_null_string(title2):
                job_titles.append(title1)
            else:
                job_titles.append('{} {}'.format(title1, title2))

        for field in FIELDS:
            row[field] = (doc.get(field, None) or [""])[0]

        for field in FIELDS:
            if _is_null_string(row[field]):
                row[field] = ''

        for field, convert_value in FIELD_CONVERSION.items():
            row[field] = convert_value(row[field])

        row['PreviousPositions'] = previous_jobs_at_the_company
        row['Positions'] = job_titles


        row['SugMisparZihuy'] = SUG_MISPAR_ZIHUY_MAPPING[row['SugMisparZihuy']]

        yield row




def flow(*_):
    return Flow(
        filter_by_type,
        rename_fields(RENAME_FIELDS),
        add_fields(FIELDS, 'string'),
        validate,
        parse_document,
        delete_fields(['document', 'pdf', 'other', 'num_files', 'parser_version', 'source', 's3_object_name']),
    )


if __name__ == '__main__':
    csv.field_size_limit(512 * 1024)

    Flow(
        load('/var/datapackages/maya/maya_complete_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()
