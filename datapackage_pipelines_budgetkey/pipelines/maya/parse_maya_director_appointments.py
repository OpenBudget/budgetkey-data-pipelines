# coding=utf-8
from dataflows import Flow, printer, delete_fields, load
import csv
from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser_utils import verify_row_count, \
    fix_fields, add_fields, rename_fields, verify_row_values

SUG_MISPAR_ZIHUY_MAPPING = {'מספר ת.ז.': 'id_number',
                            'ID no.':'id_number',
                            'מספר דרכון': 'passport_number',
                            'Passport no.': 'passport_number',
                            'מספר ביטוח לאומי':'Social Security Number',
                            'מספר זיהוי לא ידוע': 'unknown'
                            }

YES_NO_MAPPING = {
    '_________': None,
    'כן':True,
    'לא':False,
    'הינו':True,
    'הנו':True,
    'אינו':False,
    'is not': False,
    'is': True,
    'אינו ממלא':False,
    'ממלא':True,
    'hold': True,
    'Hold': True,
    'holds':True,
    'Holds':True,
    'Yes': True,
    'yes': True,
    'No': False,
    'no': False,
    'does not hold': False,
    'Does not hold': False,
    'מחזיק': True,
    'אינו מחזיק': False,
    'does not serve': False,
    'serves': True,
    'אינו מכהן': False,
    'מכהן': True
}

CITIZENSHIP_MAPPING = {
    'אדם פרטי ללא אזרחות ישראלית': False,
    'אדם פרטי עם אזרחות ישראלית' :True,
    'Individual who holds Israeli citizenship' :True,
    'Individual who does not hold Israeli citizenship' :False
}


EMPTY_STRING = '_________'


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
    'TextHofshi': 'Comments',
    'Hadirector2': 'IsDirectorInAnotherCompany',
    'Hesber1': 'DirectorAtAnotherCompanyExplanation',
    'Director3': 'IsDirectorEmployedAtCompanyOrConnectedCompany',
    'Hesber3': 'DirectorEmployedAtConnectedCompanyExplanation',
    'MisparZihuy1': 'MisparZihuy',
    'SugMisparZihuy1':'SugMisparZihuy',
    'TaarichLeida': 'BirthDate',
    'Tafkid': 'JobTitle',
    'TeurTafkid': 'JobDescription',
    'Table12': 'IsBothDirectorAndCeoOrRelativeOfCeo',
    'OtherRolesInSociety': 'EmployedAtAnotherJobConnectedToTheCompany',
    'Detail': 'EmployedAtAnotherJobConnectedToTheCompanyExplanation',
    'MunaLeMachlifShel':'IsTemporaryReplacmentOf',
    'TekufatKehunato': 'ReplacmentEndDate',
    'Director4':'RelativeOfAnotherVip',
    'Hesber4':'RelativeOfVipExplanation',
    'Director6':'IsMemberInBoardCommittee',
    'NoseMisraBechira3': 'DirectorHoldsCompanyStocks',
    'Hsba1':'StockVotingPower',
    'Hsba2':'StockVotingPowerAfterDilution',
    'NoseMisraBechira4': 'DirectorHoldsStocksInSubsidiaryOrConnectedCompany',
    'BaalMeyumanutCheshbonaitOLo': 'HasAccountingExpertise'
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
    'AppointmentApprovalDate',
    'Comments',
    'IsDirectorInAnotherCompany',
    'DirectorAtAnotherCompanyExplanation',
    'IsDirectorEmployedAtCompanyOrConnectedCompany',
    'DirectorEmployedAtConnectedCompanyExplanation',
    'EmployedAtAnotherJobConnectedToTheCompany',
    'EmployedAtAnotherJobConnectedToTheCompanyExplanation',
    'BirthDate',
    'IsBothDirectorAndCeoOrRelativeOfCeo',
    'IsTemporaryReplacmentOf',
    'ReplacmentEndDate',
    'RelativeOfAnotherVip',
    'RelativeOfVipExplanation',
    'IsMemberInBoardCommittee',
    'DirectorHoldsCompanyStocks',
    'DirectorHoldsStocksInSubsidiaryOrConnectedCompany',

    'HasAccountingExpertise',
    'IsIndependentDirector',
    'HeaderMisparBaRasham',
    'HeaderSemelBursa',
    'KodSugYeshut',
    'MezahehHotem',
    'MezahehTofes',
    'NeyarotErechReshumim',
    'PumbiLoPumbi']

TABLE_FIELDS = [    'JobTitle',
                    'PreviousPositions',
                    'StockVotingPower',
                    'StockVotingPowerAfterDilution',]

def validate(rows):
    for row in rows:
        verify_row_count(row, FIELDS, 1)
        verify_row_values(row, 'IsIsraeli', CITIZENSHIP_MAPPING)
        verify_row_values(row, 'SugMisparZihuy', SUG_MISPAR_ZIHUY_MAPPING)
        verify_row_values(row, 'IsDirectorInAnotherCompany', YES_NO_MAPPING)
        verify_row_values(row, 'IsCEOOrCEORelative', YES_NO_MAPPING)
        verify_row_values(row, 'IsDirectorEmployedAtCompanyOrConnectedCompany', YES_NO_MAPPING)
        verify_row_values(row, 'EmployedAtAnotherJobConnectedToTheCompany', YES_NO_MAPPING)

        verify_row_values(row, 'RelativeOfAnotherVip', YES_NO_MAPPING)
        verify_row_values(row, 'IsMemberInBoardCommittee', YES_NO_MAPPING)
        verify_row_values(row, 'DirectorHoldsCompanyStocks', YES_NO_MAPPING)
        verify_row_values(row, 'DirectorHoldsStocksInSubsidiaryOrConnectedCompany', YES_NO_MAPPING)

        yield row


def filter_by_type(rows):
    for row in rows:
        # 'NoseMisraBechira3' indicates that this is a new format >2005 where the type holdings is indicated
        if row['type'] == 'ת093' and 'NoseMisraBechira3'  in row['document']:
            yield row


def _is_null_string(string):
    return not string or string == '-' * len(string) or string == '_' * len(string)


def parse_document(rows):

    for row in rows:
        doc = row['document']

        for field in FIELDS:
            row[field] = (doc.get(field, None) or [""])[0]

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
        for title1, title2 in zip(doc.get('JobTitle', []), doc.get('JobDescription', [])):
            if title1 == 'אחר':
                job_titles.append(title2)
            elif _is_null_string(title2):
                job_titles.append(title1)
            else:
                job_titles.append('{} {}'.format(title1, title2))

        total_stock_vote_power = 0
        for val in doc.get('StockVotingPower',[]):
            if _is_null_string(val):
                continue
            else:
                total_stock_vote_power += float(val)
        row['StockVotingPower'] = str(total_stock_vote_power)

        diluted_vot_power = 0
        for val in doc.get('StockVotingPowerAfterDilution',[]):
            if _is_null_string(val):
                continue
            else:
                diluted_vot_power += float(val)
        row['StockVotingPowerAfterDilution'] = str(diluted_vot_power)

        row['PreviousPositions'] = previous_jobs_at_the_company
        row['JobTitle'] = job_titles
        row['IsIsraeli'] = CITIZENSHIP_MAPPING[row['IsIsraeli']]
        row['IsDirectorInAnotherCompany'] = YES_NO_MAPPING[row['IsDirectorInAnotherCompany']]
        row['SugMisparZihuy'] = SUG_MISPAR_ZIHUY_MAPPING[row['SugMisparZihuy']]
        row['IsDirectorEmployedAtCompanyOrConnectedCompany'] = YES_NO_MAPPING[row['IsDirectorEmployedAtCompanyOrConnectedCompany']]
        row['IsBothDirectorAndCeoOrRelativeOfCeo'] = YES_NO_MAPPING[row['IsBothDirectorAndCeoOrRelativeOfCeo']]
        row['EmployedAtAnotherJobConnectedToTheCompany'] = YES_NO_MAPPING[row['EmployedAtAnotherJobConnectedToTheCompany']]
        row['RelativeOfAnotherVip'] = YES_NO_MAPPING[row['RelativeOfAnotherVip']]
        row['IsMemberInBoardCommittee'] = YES_NO_MAPPING[row['IsMemberInBoardCommittee']]
        row['DirectorHoldsCompanyStocks'] = YES_NO_MAPPING[row['DirectorHoldsCompanyStocks']]
        row['DirectorHoldsStocksInSubsidiaryOrConnectedCompany'] = YES_NO_MAPPING[row['DirectorHoldsStocksInSubsidiaryOrConnectedCompany']]
        yield row




def flow(*_):
    return Flow(
        filter_by_type,
        rename_fields(RENAME_FIELDS),
        add_fields(FIELDS, 'string'),
        add_fields(TABLE_FIELDS, 'string'),
        validate,
        parse_document,
        fix_fields(FIELDS),
        delete_fields(['document', 'pdf', 'other', 'num_files', 'parser_version', 'source', 's3_object_name']),
    )


if __name__ == '__main__':
    csv.field_size_limit(512 * 1024)

    Flow(
        load('/var/datapackages/maya/maya_complete_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()
