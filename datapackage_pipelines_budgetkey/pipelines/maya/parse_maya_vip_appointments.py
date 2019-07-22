# coding=utf-8
from dataflows import Flow, printer, delete_fields, add_field, add_computed_field, load
import csv
import logging

SUG_MISPAR_ZIHUY_MAPPING = {'מספר ת.ז.': 'id_number', 'מספר דרכון': 'passport_number', 'מספר זיהוי לא ידוע': 'unknown'}

EMPTY_STRING = '_________'

BOOLEAN_FIELDS_TO_TRUE_VALUES = {
    'IsIsraeli': 'אדם פרטי עם אזרחות ישראלית',
    'IsBothDirectorAndCeoOrRelativeOfCeo': 'כן',
    'EmployedAtAnotherJobConnectedToTheCompany': 'ממלא',
    'RelativeOfAnotherVip': 'הינו',
    'HasStocksInTheCompany': 'מחזיק',
    'HasStocksInSubsidiaryOrConnectedCompany': 'מחזיק'
}

RENAME_FIELDS = {
    'Shem': 'FullName',
    'EnglishName': 'FullNameEn',
    'ManageTypeDate': 'IndependentDirectorStartDate',
    'ShemYeshutIvrit': 'CompanyName',
    'ShemYeshutKodem': 'PreviousCompanyNames',
    'ShemYeshutLoazit': 'CompanyNameEn',
    'EretzEzrachut': 'Citizenship',
    'Ezrahut': 'IsIsraeli',
    'TaarichTchilatHaKehuna':  'AppointmentDate',
    'ApprovalDate': 'AppointmentApprovalDate',
    'IsDirectorAndCEO': 'IsBothDirectorAndCeoOrRelativeOfCeo',
    'TextHofshi': 'Comments',
    'NoseMisraBechira1': 'EmployedAtAnotherJobConnectedToTheCompany',
    'Hesber1': 'EmployedAtAnotherJobConnectedToTheCompanyExplanation',
    'NoseMisraBechira2': 'RelativeOfAnotherVip',
    'Hesber2': 'RelativeOfVipExplanation',
    'NoseMisraBechira3': 'HasStocksInTheCompany',
    'NoseMisraBechira4': 'HasStocksInSubsidiaryOrConnectedCompany',
}

FIELDS = [
    'SugMisparZihuy',
    'MisparZihuy',
    'FullName',
    'FullNameEn',
    'IndependentDirectorStartDate',
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
    'MezahehYeshut',
    'NeyarotErechReshumim',
    'PumbiLoPumbi',
    'AsmachtaDuachMeshubash',
    'TaarichIdkunMivne']

ADDITIONAL_FIELDS = ['PreviousPositions', 'Positions']


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

        for field, true_value in BOOLEAN_FIELDS_TO_TRUE_VALUES.items():
            row[field] = row[field] == true_value

        row['PreviousPositions'] = previous_jobs_at_the_company
        row['Positions'] = job_titles


        row['SugMisparZihuy'] = SUG_MISPAR_ZIHUY_MAPPING[row['SugMisparZihuy']]

        yield row


def add_fields(names, type):
    return add_computed_field([dict(target=name,
                                    type=type,
                                    operation=(lambda row: None)
                                    ) for name in names])


def rename_fields(rows):
    for row in rows:
        doc = row['document']
        for (k, v) in RENAME_FIELDS.items():
            doc[v] = doc.get(k, [])
        yield row


def flow(*_):
    return Flow(
        filter_by_type,
        rename_fields,
        add_fields(FIELDS, 'string'),
        add_fields(ADDITIONAL_FIELDS, 'string'),
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
