
from dataflows import Flow, printer, delete_fields, add_field, add_computed_field, load
import csv
import logging



RENAME_FIELDS = {
    'NoseMisra': 'FullName',
    'ShemAnglit': 'FullNameEn',
    'ManageTypeDate': 'IndependentDirectorStartDate',
    'TafkidList':'Position',
    'Tafkid': 'PositionDetails',
    'Taarich': 'Date',
    'ShemYeshutIvrit':'CompanyName',
    'ShemYeshutKodem':'PreviousCompanyNames',
    'ShemYeshutLoazit': 'CompanyNameEn'

}

FIELDS = [
      'CompanyName',
      'CompanyUrl',
      'HeaderMisparBaRasham',
      'HeaderSemelBursa',
      'KodSugYeshut',
      'MezahehHotem',
      'MezahehTofes',

      'NeyarotErechReshumim',
      'PumbiLoPumbi',
      'PreviousCompanyNames',
      'TaarichIdkunMivne']

OPTIONAL_FIELDS = ['AsmachtaDuachMeshubash', 'Date', 'MezahehYeshut']

TABLE_FIELDS = ['FullName',
                'FullNameEn',
                'AppointmentDate',
                'SugMisparZihui',
                'MisparZihui',
                'Position',
                'PositionDetails',
                'CompensationCommittee',
                'IsFinancialExpert',
                'IsInspectionComitee',
                'IsOftheAuditCommittee',
                'OtherCommittees',
                'IndependentDirectorStartDate',
                ]


def verify_row_count(row, expected_fields, expected_count, is_required=True):
    doc = row['document']
    url = row['url']
    for field in expected_fields:
        if not is_required and field not in doc:
            return
        if field not in doc:
            raise Exception("Document {} failed validation. textAlias {} not found. (expected: {})".format(url, field, expected_count))
        elif len(doc[field]) != expected_count:
            raise Exception("Document {} failed validation. textAlias {} found {} times. (expected: {})".format(url, field, len(doc[field]), expected_count))


def validate(rows):
    for row in rows:
        doc = row['document']
        verify_row_count(row, FIELDS, 1)
        verify_row_count(row, OPTIONAL_FIELDS, 1, is_required=False)
        num_officers = len(doc.get('FullName', []))
        verify_row_count(row, TABLE_FIELDS, num_officers)
        yield row

def filter_by_type(rows):
    for row in rows:
        if row['type'] in ['ת097']:
            yield row


def parse_document(rows):
    for row in rows:
        doc = row['document']

        fields = {k:doc.get(k, []) for k in TABLE_FIELDS }

        num_officers = max(len(fields[k]) for k in TABLE_FIELDS  )
        for field in FIELDS:
            row[field] = doc[field][0]
        for field in OPTIONAL_FIELDS:
            row[field] = doc[field][0] if doc.get(field) else None
        for i in range(num_officers):
            officer_fields = {k:(fields[k][i] if len(fields[k]) >i else "") for k in TABLE_FIELDS}
            new_record = {}
            new_record.update(row)
            new_record.update(officer_fields)
            yield new_record

def add_fields(names, type):
    return add_computed_field( [dict(target=name,
                                     type=type,
                                     operation=(lambda row: None)
                                     ) for name in names])
def rename_fields(rows):
    for row in rows:
        doc = row['document']
        for (k,v) in RENAME_FIELDS.items():
            doc[v] = doc.get(k,[])
        yield row

def fix_fields(rows):
    for row in rows:
        for f in TABLE_FIELDS:
            if row[f] == ('_' * len(row[f])):
                row[f] = ""
            if row[f] == ('-' * len(row[f])):
                row[f] = ""
        yield row

def flow(*_):
    return Flow(
        filter_by_type,
        rename_fields,
        add_fields(FIELDS, 'string'),
        add_fields(OPTIONAL_FIELDS, 'string'),
        add_fields(TABLE_FIELDS,'string'),
        validate,
        parse_document,
        fix_fields,
        delete_fields(['document', 'pdf', 'other', 'num_files', 'parser_version', 'source', 's3_object_name']),
    )

if __name__ == '__main__':
    csv.field_size_limit(512*1024)
    Flow(
        load('/var/datapackages/maya/maya_complete_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()

