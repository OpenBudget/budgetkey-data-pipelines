# coding=utf-8
from dataflows import Flow, printer, delete_fields, update_resource, load

from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser_utils import verify_row_count, add_fields, \
    rename_fields, verify_row_values, fix_fields

import csv

ID_TYPE_MAPPING = {'מספר מזהה אחר': 'other',
                   'מספר רשם בארץ ההתאגדות בחו"ל': 'id_at_foreign_corporation_registry',
                   'מספר ברשם החברות בישראל': 'id_at_corporation_registry',
                   'מספר ברשם השותפויות בישראל': 'id_at_association_registry',
                   'מספר תעודת זהות':'id_number'}

HOLDER_TYPE_MAPPING = {'החברה':False,
                       'חברת בת של החברה':True }

RENAME_FIELDS = {
    'HolderType': 'IsHeldBySubsidiary',
    'ShemChevratBat': 'SubsidiaryName',
    'CompanyEnglishName': 'SubsidiaryNameEn',
    'ShemYeshutKodem':'PreviousCompanyNames',
    'ShemYeshutLoazit': 'CompanyNameEn',

    'ShiurHachzaka': 'PercentageHeld',
    'IDType': 'Subsidiary_IDType',
    'IdNumber': 'Subsidiary_IDNumber',
    'MisparNiyarBursa': 'StockNumber',
    'TheNameOfTheChangedSecuritie': 'ChangedStockName',
    'SecuritiesAmountChange': 'StockAmountChange',
    'SecuritiesHolderBalanceAfterChange': 'NumberOfStocksAfterChange',
}

FIELDS = [
    'CompanyNameEn',
    'CompanyUrl',
    'HeaderMisparBaRasham',
    'HeaderSemelBursa',
    'KodSugYeshut',
    'MezahehHotem',
    'MezahehTofes',
    'NeyarotErechReshumim',
    'PumbiLoPumbi',
    'PreviousCompanyNames',
]

TABLE_FIELDS = [
    'IsHeldBySubsidiary',
    'SubsidiaryName',
    'SubsidiaryNameEn',
    'PercentageHeld',
    'Subsidiary_IDType',
    'Subsidiary_IDNumber',
    'StockNumber',
    'ChangedStockName',
    'ChangeDate',

    'StockAmountChange',
    'NumberOfStocksAfterChange',
    'CitizenshipOfRegistering',
    'countryOfRegistering',

    'Table776',
    'Table774',
    'Table775',
    'OtherChange'
]

OPTIONAL_TABLE_FIELDS = ['Proceeds']

ADDITIONAL_FIELDS = ['ChangeExplanation']


def filter_by_type(rows):
    for row in rows:
        if row['type'] == 'ת086':
            yield row


def _is_null_string(string):
    return not string or string == '-' * len(string) or string == '_' * len(string)



def validate(rows):
    for row in rows:
        doc = row['document']
        num_records = len(doc.get(TABLE_FIELDS[0], []))

        verify_row_count(row, FIELDS, 1)
        verify_row_count(row, TABLE_FIELDS, num_records)
        verify_row_count(row, OPTIONAL_TABLE_FIELDS, num_records, is_required=False)
        verify_row_values(row, 'IsHeldBySubsidiary', HOLDER_TYPE_MAPPING)
        verify_row_values(row, 'Subsidiary_IDType', ID_TYPE_MAPPING)
        yield row

def parse_document(rows):
    for row in rows:
        doc = row['document']
        for field in FIELDS:
            row[field] = doc[field][0]

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


            new_record['PercentageHeld'] = new_record['PercentageHeld'].replace('%', '')
            new_record['Subsidiary_IDType'] = ID_TYPE_MAPPING[new_record['Subsidiary_IDType']]
            new_record['IsHeldBySubsidiary'] = HOLDER_TYPE_MAPPING[new_record['IsHeldBySubsidiary']]
            new_record['ChangeExplanation'] = ' '.join(x for x in
                                            [new_record['Table776'], new_record['Table774'], new_record['Table775'], new_record['OtherChange']]
                                            if not _is_null_string(x))

            yield new_record


def flow(*_):
    return Flow(
        update_resource(
            -1, name='maya_holdings_change', path="data/maya_holdings_change.csv",
        ),
        filter_by_type,
        add_fields(FIELDS, 'string'),
        add_fields(TABLE_FIELDS, 'string'),
        add_fields(OPTIONAL_TABLE_FIELDS,'string'),
        add_fields(ADDITIONAL_FIELDS, 'string'),
        rename_fields(RENAME_FIELDS),
        fix_fields(FIELDS + TABLE_FIELDS + OPTIONAL_TABLE_FIELDS + ADDITIONAL_FIELDS),
        validate,
        parse_document,
        delete_fields(['document',
                       'pdf',
                       'other',
                       'num_files',
                       'parser_version',
                       'source',
                       's3_object_name',
                       'Table776',
                       'Table774',
                       'Table775',
                       'OtherChange']),
    )


if __name__ == '__main__':
    csv.field_size_limit(512 * 1024)

    Flow(
        load('/var/datapackages/maya/maya_complete_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()
