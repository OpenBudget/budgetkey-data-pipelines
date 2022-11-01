# coding=utf-8
from dataflows import Flow, printer, delete_fields, update_resource, add_computed_field, load
import csv
from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser_utils import rename_fields, add_fields, fix_fields, \
    verify_row_count, verify_same_row_count, verify_row_values

SUG_MISPAR_ZIHUY_MAPPING = {'מספר ת.ז.': 'id_number',
                            'ID no.':'id_number',
                            'מספר דרכון': 'passport_number',
                            'Passport no.': 'passport_number',
                            'מספר ביטוח לאומי':'social_security_number',
                            'מספר זיהוי לא ידוע': 'unknown'
                            }

RENAME_FIELDS = {
    'Shem': 'FullName',
    'ShemPriatiVeMishpacha': 'FullName',

    'EnglishName': 'FullNameEn',

    'TaarichTchilatHaKehuna': 'ReportDate',
    'TaarichTchilatHaCehuna': 'ReportDate',

    'HaTafkidSheMila': 'Position',
    'MekomHaAvoda': 'CompanyName',

    'MeshechZmanSheMilaBaTafkid': 'DurationInPosition/When',
    'MeshechHaZmanSheMilaTafkid': 'DurationInPosition/When',

    'MisparZihuy1': 'MisparZihuy',
    'SugMisparZihuy1':'SugMisparZihuy',
}

FIELDS = [
    'SugMisparZihuy',
    'MisparZihuy',
    'FullName',
    'ReportDate'
]

OPTIONAL_FIELDS = ['FullNameEn']

TABLE_FIELDS = [
    'Position',
    'CompanyName',
    'DurationInPosition/When'
]

def validate(rows):
    for row in rows:
        verify_row_count(row, FIELDS, 1)
        verify_row_count(row, OPTIONAL_FIELDS, 1, is_required=False)
        verify_same_row_count(row, TABLE_FIELDS)
        verify_row_values(row, 'SugMisparZihuy', SUG_MISPAR_ZIHUY_MAPPING)
        yield row


def filter_by_type(rows):
    for row in rows:
        if row['type'] == 'ת091' or row['type'] == 'ת093':
            yield row


def _is_null_string(string):
    return not string or string == '-' * len(string) or string == '_' * len(string)


def parse_document(rows):
    for row in rows:
        doc = row['document']


        table_elements = len(doc.get(TABLE_FIELDS[0],[]))
        for field in FIELDS:
            row[field] = doc[field][0]
        for field in OPTIONAL_FIELDS:
            row[field] = doc[field][0] if doc.get(field) else None
        row['SugMisparZihuy'] = SUG_MISPAR_ZIHUY_MAPPING[row['SugMisparZihuy']]

        for i in range(table_elements):
            work_record = {k:doc[k][i]  for k in TABLE_FIELDS}
            new_record = {}
            new_record.update(row)
            new_record.update(work_record)
            yield new_record

        #Add an empty record in case user did not fill any work record
        if table_elements == 0:
            new_record = {}
            new_record.update(row)
            new_record.update({k:""  for k in TABLE_FIELDS})
            yield new_record



def flow(*_):
    return Flow(
        update_resource(
            -1, name='reported_work_record', path="data/reported_work_record.csv",
        ),
        filter_by_type,
        rename_fields(RENAME_FIELDS),
        add_fields(FIELDS, 'string'),
        add_fields(TABLE_FIELDS, 'string'),
        add_fields(OPTIONAL_FIELDS, 'string'),
        validate,
        parse_document,
        fix_fields(TABLE_FIELDS + OPTIONAL_FIELDS + FIELDS),
        delete_fields(['document', 'pdf', 'other', 'num_files', 'parser_version', 'source', 's3_object_name', 'id', 'company', 'type', 'fix_for', 'fixed_by', 'next_doc', 'prev_doc']),
    )


if __name__ == '__main__':
    csv.field_size_limit(512 * 1024)

    Flow(
        load('/var/datapackages/maya/maya_complete_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()
