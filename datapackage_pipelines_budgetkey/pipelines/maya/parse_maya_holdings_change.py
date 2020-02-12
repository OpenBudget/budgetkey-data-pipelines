# coding=utf-8
from dataflows import Flow, printer, delete_fields, update_resource, load

from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser_utils import verify_row_count, add_fields, \
    rename_fields, verify_row_values, fix_fields

import csv


RENAME_FIELDS = {
    'ShemYeshutIvrit':'CompanyName',
    'ShemYeshutKodem':'PreviousCompanyNames',
    'ShemYeshutLoazit': 'CompanyNameEn',
    'Date': 'HoldingsChangeDate', #התאריך בהם נודע לתאגיד לראשונה על הארוע
    'TextHofshi': 'Comments',
    'Shem': 'FullName',
    'EnglishName': 'FullNameEn',
    'ErezEzrahutSlashHitagdut' :'Nationality',
    'ErechTavle122nd' : 'IsFrontForOthers',
    'MahutShinui1': 'ChangeType',
    'MahutShinui2': 'IncreaseReason',
    'MahutShinui3': 'DecreaseReason',
    'Aher': 'FreeTextReason',
    'EzrahutSlashErez': 'EntityType',
    'table757': 'HolderType',
    'ErechTavla121st': 'IsNostroAccountOfBankOrInsuranceCompany',

}

FIELDS = [
    'Comments', #Additional noted in free text
    'PreviousCompanyNames',
    'CompanyNameEn',
    'MezahehHotem',
    'MoedHashlamatTashlum', #אם לא שולמה כל התמורה במועד השינוי נא לציין את מועד השלמת התשלום
    'MezahehTofes',
    'KodSugYeshut',
    'PumbiLoPumbi',
    'HeaderSemelBursa',
    'NeyarotErechReshumim',
    'HoldingsChangeDate',
    'CompanyName',
    'OfenSiumHashala', #אם השינוי הוא בדרך של חתימה על כתב ההשאלה נא לציין פרטים בדבר אופן סיום ההשאלה
    'HeaderMisparBaRasham',

]

OPTIONAL_FIELDS = [
    'MezahehYeshut',
    'CompanyUrl',
    'DataToEstimateValue', #פירוט הפעולות שגרמו לשינוי
]

TABLE_FIELDS = [
    'FullName',
    'SugMisparZihui',
    'MisparZihui',
    'EntityType',
    'MisparNiarErech',
    'IsFrontForOthers',
    'Nationality',
    'ItraKodemet',
    'ItraLeaharShinui',
    #'GidulOKitun',
    'ShiurAhzakaBedilulBehon',
    'ShiurAhzakaBedilulBekoahHazbaa',
    'ShiurAhzakaBehon',
    'ShaarIska',
    'Matbea',
    'ChangeType',
    'FreeTextReason',
    'HeyotanMenayotRedumot',
    'ShemVeSugNiarErech'

]

#SubtractionAdditionTable + GidulOKitun

#BiorTbl
OPTIONAL_TABLE_FIELDS = ['IsNostroAccountOfBankOrInsuranceCompany', 'FullNameEn', 'EntityType', 'MisparBior',
                         'InterestedShareholdeName',
                         'InterestedShareholdeID',]

ADDITIONAL_FIELDS = ['FooterMisparAsmachta1', 'FooterMisparAsmachta2', 'FooterMisparAsmachta3']

#IsNostroAccountOfBankOrInsuranceCompany
YES_NO_VALUES = {'כן': True, 'לא': False}

#EntityType
ENTITY_TYPES = {'התאגד בישראל': 'Israeli Corporation',
 'אדם פרטי עם אזרחות ישראלית': 'Israeli Citizen',
 'אדם פרטי ללא אזרחות ישראלית': 'Foreign Citizen',
 'התאגד בחו"ל': 'Foreign Corporation'}

def filter_by_type(rows):
    for row in rows:
        if row['type'] == 'ת076':
            yield row


def _is_null_string(string):
    return not string or string == '-' * len(string) or string == '_' * len(string)



def validate(rows):
    for row in rows:
        doc = row['document']
        num_records = len(doc.get(TABLE_FIELDS[0], []))

        verify_row_count(row, FIELDS, 1)
        verify_row_count(row, OPTIONAL_FIELDS, 1, is_required=False)
        verify_row_count(row, ADDITIONAL_FIELDS, 1, is_required=False)
        verify_row_count(row, TABLE_FIELDS, num_records)
        verify_row_count(row, OPTIONAL_TABLE_FIELDS, num_records, is_required=False)
        verify_row_values(row, 'IsNostroAccountOfBankOrInsuranceCompany', YES_NO_VALUES)
        verify_row_values(row, 'EntityType', ENTITY_TYPES)
        yield row

def parse_document(rows):
    for row in rows:
        doc = row['document']
        for field in FIELDS:
            row[field] = doc[field][0]
        for field in OPTIONAL_FIELDS + ADDITIONAL_FIELDS:
            row[field] = doc.get(field,[None])[0]

        for idx in range(len(doc[TABLE_FIELDS[0]])):
            new_record = {}
            new_record.update(row)
            new_record.update({k: doc[k][idx] for k in TABLE_FIELDS})
            new_record.update({k: doc[k][idx] for k in OPTIONAL_TABLE_FIELDS if k in doc})

            for field in TABLE_FIELDS:
                if _is_null_string(new_record[field]):
                    new_record[field] = ''

            for field in OPTIONAL_TABLE_FIELDS:
                if field in new_record and _is_null_string(new_record[field]):
                    new_record[field] = ''

            if new_record.get('IsNostroAccountOfBankOrInsuranceCompany',None):
                new_record['IsNostroAccountOfBankOrInsuranceCompany'] = YES_NO_VALUES[new_record['IsNostroAccountOfBankOrInsuranceCompany']]
            new_record['EntityType'] = ENTITY_TYPES[new_record['EntityType']]
            new_record['AdditionalRecords'] = ' '.join(x for x in
                                                       [new_record['FooterMisparAsmachta1'], new_record['FooterMisparAsmachta2'], new_record['FooterMisparAsmachta3']]
                                                       if not _is_null_string(x))

            yield new_record


def flow(*_):
    return Flow(
        update_resource(
            -1, name='maya_holdings_change', path="data/maya_holdings_change.csv",
        ),
        filter_by_type,
        add_fields(FIELDS + OPTIONAL_FIELDS +  TABLE_FIELDS + OPTIONAL_TABLE_FIELDS, 'string'),
        rename_fields(RENAME_FIELDS),
        fix_fields(FIELDS + OPTIONAL_FIELDS +  TABLE_FIELDS + OPTIONAL_TABLE_FIELDS),
        validate,
        parse_document,
        delete_fields(['document',
                       'pdf',
                       'other',
                       'num_files',
                       'parser_version',
                       'source',
                       's3_object_name',
                       ]),
    )


if __name__ == '__main__':
    csv.field_size_limit(512 * 1024)

    Flow(
        load('/var/datapackages/maya/maya_complete_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()
