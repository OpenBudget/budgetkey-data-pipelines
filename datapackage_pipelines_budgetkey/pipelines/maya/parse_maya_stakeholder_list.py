
from dataflows import Flow, printer, delete_fields, add_field, add_computed_field, load, set_type
import csv
import logging

CORE_STAKE_HOLDER_TEXT = 'בעלי עניין בתאגיד בעלי אמצעי שליטה מהותי'
CORE_STAKE_HOLDER_FIELDS = {
    'ShemVeSugNiarErech': 'StockName',
    'ItraHadasha': 'CurrentAmount',
    'ShiurAhzakaBehon': 'CapitalPct',
    'ShiurAhzakaBekoahHazbaa': 'VotePower',
    'ShiurAhzakaBedilulBehon': 'CapitalPct_Dilul',
    'ShiurAhzakaBedilulBekoahHazbaa': 'VotePower_Dilul',
}

OFFICER_STAKE_HOLDER_TEXT ='נושאי משרה בכירה בתאגיד ללא אמצעי שליטה מהותי'
OFFICER_STAKE_HOLDER_FIELDS = {
    'ShemVeSugNiarErechBaelyEnyan': 'StockName',
    'ItraHadashaBaelyEnyan': 'CurrentAmount',
    'ShiurAhzakaBedilulBehonNosheMisra': 'CapitalPct',
    'ShiurAhzakaBekoahHazbaaNosheMisra': 'VotePower',
    'ShiurAhzakaBekoahHazbaaNosheMisra1': 'CapitalPct_Dilul',
    'ShiurAhzakaBedilulBekoahHazbaaNosheMisra': 'VotePower_Dilul',
}


RENAME_FIELDS = {
    'ShemHamahzik': 'FullName',
    'ShemMahzikAnglit' : 'FullNameEn',
    'ErechTavla642' : 'Position',
    'EzrahutSlashErez' :'Nationality',
    'HaimMahzik' : 'IsFrontForOthers',
    'IsHolderReport' : 'IsRequiredToReportChange',
    'MenayotMohzakot' : 'TreasuryShares',
    'ItraBeDivuachKodem' : 'PreviousAmount',
    'GidulOKitun' : 'ChangeSincePrevious',
    'Harot' : 'Notes',
    'Tarich': 'Date',
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
      'MezahehYeshut',
      'NeyarotErechReshumim',
      'PumbiLoPumbi',
      'AsmachtaDuachMeshubash',
      'PreviousCompanyNames',
      'Date']

TABLE_FIELDS = ['FullName',
                'FullNameEn',
                'Position',
                'SugMisparZihui',
                'MisparZihui',
                'Nationality',
                'IsFrontForOthers',
                'IsRequiredToReportChange',
                'TreasuryShares',
                'PreviousAmount',
                'ChangeSincePrevious',
                'MisparNiarErech',
                'MaximumRetentionRate',
                'MinimumRetentionRate',
                'Notes',
                ]


def filter_by_type(rows):
    for row in rows:
        if row['type'] == 'ת077':
            yield row


def parse_document(rows):
    for row in rows:
        doc = row['document']

        for field in FIELDS:
            try:
                row[field] = doc.get(field,[""])[0]
            except:
                logging.warning(field)
                raise
        stakeholder_fields = {k:doc.get(k, []) for k in TABLE_FIELDS }
        num_stakeholders = max(len(stakeholder_fields[k]) for k in stakeholder_fields  )

        core_stakeholder_fields = {v:doc.get(k, []) for k,v in CORE_STAKE_HOLDER_FIELDS.items() }
        num_core_stakeholders = max(len(core_stakeholder_fields[k]) for k in core_stakeholder_fields  )
        officer_stakeholder_fields = {v:doc.get(k, []) for k,v in OFFICER_STAKE_HOLDER_FIELDS.items() }


        for i in range(num_stakeholders):
            new_fields = {k:(stakeholder_fields[k][i] if len(stakeholder_fields[k]) >i else "") for k in TABLE_FIELDS}
            if i < num_core_stakeholders:
                new_fields['stakeholder_type'] = CORE_STAKE_HOLDER_TEXT
                new_fields.update({k:(core_stakeholder_fields[k][i]) for k in core_stakeholder_fields.keys()})
            else:
                new_fields['stakeholder_type'] = OFFICER_STAKE_HOLDER_TEXT
                idx = i -num_stakeholders
                new_fields.update({k:(officer_stakeholder_fields[k][idx]) for k in officer_stakeholder_fields.keys()})

            new_record = {}
            new_record.update(row)
            new_record.update(new_fields)
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
        for f in row.keys():
            if isinstance(row[f], str) and row[f] == ('_' * len(row[f])):
                row[f] = None
            elif isinstance(row[f], str) and row[f] == ('-' * len(row[f])):
                row[f] = None
        yield row
def flow(*_):
    return Flow(
        filter_by_type,
        rename_fields,
        add_field('stakeholder_type', 'string'),
        add_fields(FIELDS, 'string'),
        add_fields(TABLE_FIELDS,'string'),
        add_fields(CORE_STAKE_HOLDER_FIELDS.values(),'string'),
        parse_document,
        delete_fields(['document', 'pdf', 'other', 'num_files', 'parser_version', 'source', 's3_object_name']),
        fix_fields,
        set_type('CapitalPct', type='number'),
        set_type('VotePower', type='number'),
        set_type('CapitalPct_Dilul', type='number'),
        set_type('VotePower_Dilul', type='number'),

    )


if __name__ == '__main__':
    csv.field_size_limit(512*1024)

    Flow(
        load('/var/datapackages/maya/maya_complete_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()

