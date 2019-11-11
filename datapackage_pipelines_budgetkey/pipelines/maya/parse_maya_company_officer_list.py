
from dataflows import Flow, printer, delete_fields, update_resource, load
import csv
from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser_utils import verify_row_count, add_fields, rename_fields, fix_fields, verify_same_row_count


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


def validate(rows):
    for row in rows:
        verify_row_count(row, FIELDS, 1)
        verify_row_count(row, OPTIONAL_FIELDS, 1, is_required=False)
        verify_same_row_count(row, TABLE_FIELDS)
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


def flow(*_):
    return Flow(
        update_resource(
            -1, name='maya_company_officer_list', path="data/maya_company_officer_list.csv",
        ),
        filter_by_type,
        rename_fields(RENAME_FIELDS),
        add_fields(FIELDS, 'string'),
        add_fields(OPTIONAL_FIELDS, 'string'),
        add_fields(TABLE_FIELDS,'string'),
        validate,
        parse_document,
        fix_fields(TABLE_FIELDS),
        delete_fields(['document', 'pdf', 'other', 'num_files', 'parser_version', 'source', 's3_object_name']),
    )

if __name__ == '__main__':
    csv.field_size_limit(512*1024)
    Flow(
        load('/var/datapackages/maya/maya_complete_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()

