# coding=utf-8
from dataflows import Flow, printer, delete_fields, update_resource, load
from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser_utils import verify_row_count, \
    add_fields, rename_fields, verify_row_values

import csv

ID_TYPE_MAPPING = {'מספר מזהה אחר': 'other',
                   'מספר רשם בארץ ההתאגדות בחו"ל': 'id_at_foreign_corporation_registry',
                   'מספר ברשם החברות בישראל': 'id_at_corporation_registry',
                   'מספר ברשם השותפויות בישראל': 'id_at_association_registry',
                   'מספר תעודת זהות':'id_number'}

BOOLEAN_FIELDS_TO_TRUE_VALUES = {
    'IsHeldBySubsidiary': 'חברת בת של החברה',
}

RENAME_FIELDS = {
    'HolderType': 'IsHeldBySubsidiary',
    'ShemChevratBat': 'CompanyName',
    'CompanyEnglishName': 'CompanyNameEn',
    'ShiurHachzaka': 'PercentageHeld',
    'IDType': 'SugMisparZihuy',
    'IdNumber': 'MisparZihuy',
    'MisparNiyarBursa': 'StockNumber',
    'TheNameOfTheChangedSecuritie': 'ChangedStockName',
    'SecuritiesAmountChange': 'StockAmountChange',
    'SecuritiesHolderBalanceAfterChange': 'NumberOfStocksAfterChange',
}

TABLE_FIELDS = [
    'IsHeldBySubsidiary',
    'CompanyName',
    'CompanyNameEn',
    'PercentageHeld',
    'SugMisparZihuy',
    'MisparZihuy',
    'StockNumber',
    'ChangedStockName',
    'ChangeDate',

    'StockAmountChange',
    'StockHeldChange',
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
        num_records = len(doc.get('MisparZihuy', []))
        verify_row_count(row, TABLE_FIELDS, num_records)
        verify_row_count(row, OPTIONAL_TABLE_FIELDS, num_records, is_required=False)
        verify_row_values(row, 'SugMisparZihuy', ID_TYPE_MAPPING)
        yield row

def parse_document(rows):
    for row in rows:
        doc = row['document']
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

            for field, true_value in BOOLEAN_FIELDS_TO_TRUE_VALUES.items():
                row[field] = row[field] == true_value


            new_record['PercentageHeld'] = new_record['PercentageHeld'].replace('%', '')
            new_record['SugMisparZihuy'] = ID_TYPE_MAPPING[new_record['SugMisparZihuy']]
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
        rename_fields(RENAME_FIELDS),
        add_fields(TABLE_FIELDS, 'string'),
        add_fields(OPTIONAL_TABLE_FIELDS,'string'),
        add_fields(ADDITIONAL_FIELDS, 'string'),
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
