# coding=utf-8
from dataflows import Flow, printer, delete_fields, add_computed_field, load
import csv

ID_TYPE_MAPPING = {'מספר מזהה אחר': 'other', 'מספר רשם בארץ ההתאגדות בחו"ל': 'id_at_foreign_corporation_registry',
                   'מספר ברשם החברות בישראל': 'id_at_corporation_registry',
                   'מספר ברשם השותפויות בישראל': 'id_at_association_registry'}

BOOLEAN_FIELDS_TO_TRUE_VALUES = {
    'IsHeldBySubsidiary': 'חברת בת של החברה',
}

RENAME_FIELDS = {
    'HolderType': 'IsHeldBySubsidiary',
    'ShemChevratBat': 'CompanyName',
    'CompanyEnglishName': 'CompanyNameEn',
    'ShiurHachzaka': 'PercentageHeld',
    'IDType': 'CompanyIDType',
    'IdNumber': 'CompanyID',
    'MisparNiyarBursa': 'StockNumber',
    'TheNameOfTheChangedSecuritie': 'ChangedStockName',
    'SecuritiesAmountChange': 'StockAmountChange',
    'SecuritiesHolderBalanceAfterChange': 'StockHeldChange',
}

FIELDS = [
    'IsHeldBySubsidiary',
    'CompanyName',
    'CompanyNameEn',
    'PercentageHeld',
    'CompanyIDType',
    'CompanyID',
    'StockNumber',
    'ChangedStockName',
    'ChangeDate',
    'Proceeds',
    'StockAmountChange',
    'StockHeldChange',

    'CitizenshipOfRegistering',
    'countryOfRegistering',
]

ADDITIONAL_FIELDS = ['ChangeExplanation']


def filter_by_type(rows):
    for row in rows:
        if row['type'] == 'ת086':
            yield row


def _is_null_string(string):
    return not string or string == '-' * len(string) or string == '_' * len(string)


def parse_document(rows):
    for row in rows:
        doc = row['document']
        for subset_index in range(len(doc['IdNumber'])):
            for field in FIELDS:
                values = doc.get(field, None) or []
                if len(values) > subset_index:
                    row[field] = values[subset_index]

            for field in FIELDS:
                if _is_null_string(row[field]):
                    row[field] = ''

            for field, true_value in BOOLEAN_FIELDS_TO_TRUE_VALUES.items():
                row[field] = row[field] == true_value

            row['PercentageHeld'] = row['PercentageHeld'].replace('%', '')
            row['CompanyIDType'] = ID_TYPE_MAPPING[row['CompanyIDType']]
            row['ChangeExplanation'] = ' '.join(x[subset_index] for x in
                                                [doc['Table776'], doc['Table774'], doc['Table775'], doc['OtherChange']]
                                                if not _is_null_string(x[subset_index]))
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
