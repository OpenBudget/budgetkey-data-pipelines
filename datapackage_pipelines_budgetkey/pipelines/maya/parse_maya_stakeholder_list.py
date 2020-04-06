
from dataflows import Flow, printer, delete_fields, add_field, add_computed_field, load, set_type, update_resource
from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser_utils import rename_fields, add_fields, \
    verify_row_count, verify_same_row_count
import csv

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
    'ErachTavla642': 'Position',
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
      'CompanyNameEn',
      'HeaderMisparBaRasham',
      'HeaderSemelBursa',
      'KodSugYeshut',
      'MezahehHotem',
      'MezahehTofes',
      'NeyarotErechReshumim',
      'PumbiLoPumbi',
      'PreviousCompanyNames']


OPTIONAL_FIELDS = ['AsmachtaDuachMeshubash',
                   'MezahehYeshut',
                   'CompanyUrl',
                   'Date' ]

TABLE_FIELDS = ['FullName',

                'Position',
                'SugMisparZihui',
                'MisparZihui',
                'Nationality',
                'IsFrontForOthers',

                'TreasuryShares',
                'PreviousAmount',
                'ChangeSincePrevious',
                'MisparNiarErech',

                'Notes',
                ]
OPTIONAL_TABLE_FIELDS = ['IsRequiredToReportChange', 'HolderOwner', 'AccumulateHoldings',  'MaximumRetentionRate',
                         'MinimumRetentionRate', 'FullNameEn']

KNOWN_CORRUPT_INSTANCES = {
    '520041641': (['משה גוב','משה גוב'],1),
    '520043829': (['יופיטר החזקה בע"מ','ארלינגטון בע"מ','בועז וקסמן','רחל וקסמן','דנה וקסמן' ,'מוד אידאה אינטרנשיונל בע"מ'],5),
    '520038787': (['בנק לאומי לישראל בע"מ', 'קיבוץ כפר עזה', 'כפרית תעשיות ואחזקות(1998) בע"מ'], 1)
}


def first(x):
    return next(iter(x))

def validate(rows):
    for row in rows:
        doc = row['document']
        url = row['url']

        verify_row_count(row, FIELDS, 1)
        verify_row_count(row, OPTIONAL_FIELDS, 1, is_required=False)
        num_stakeholders = len(doc.get(TABLE_FIELDS[0], []))
        verify_row_count(row, TABLE_FIELDS, num_stakeholders)
        verify_row_count(row, OPTIONAL_TABLE_FIELDS, num_stakeholders, is_required=False)

        verify_same_row_count(row, CORE_STAKE_HOLDER_FIELDS.keys())
        num_core_stake_holders = len(doc.get(first(CORE_STAKE_HOLDER_FIELDS.keys()),[]))

        verify_same_row_count(row, OFFICER_STAKE_HOLDER_FIELDS.keys())
        num_officer_stake_holders = len(doc.get(first(OFFICER_STAKE_HOLDER_FIELDS.keys()), []))

        is_empty_officer_list = num_officer_stake_holders==1 and not doc['ShemVeSugNiarErechBaelyEnyan'][0].replace('_','').replace('-','').strip()
        if is_empty_officer_list and (num_stakeholders != num_core_stake_holders):
            raise Exception("Documnet {} failed Validation. Expected the core list {} to match stakeholder count {}".format(url, num_stakeholders, num_core_stake_holders ))
        elif not is_empty_officer_list and (num_stakeholders != num_core_stake_holders + num_officer_stake_holders):
            raise Exception("Documnet {} failed Validation. Expected the core list {} to match stakeholder count + officer count: {} + {}".format(url, num_stakeholders, num_core_stake_holders, num_officer_stake_holders ))
        yield row

def filter_by_type(rows):
    for row in rows:
        if row['type'] == 'ת077':
            yield row

def doc_is_corrupt(doc):
    for corpNumber, (holderNames, num_stakeholders) in KNOWN_CORRUPT_INSTANCES.items():
        if doc['HeaderMisparBaRasham'][0] == corpNumber:
            if doc['HaimMahzik2'] == holderNames and len(doc['ShemHamahzik']) == num_stakeholders:
                return True
    return False

def filter_corrupt_rows(rows):
    for row in rows:
        doc = row['document']
        if 'HearatShulaim' in doc: #HearatShulaim indicates very old documents
            continue
        if doc_is_corrupt(doc):
            continue

        yield row

def parse_document(rows):
    for row in rows:
        doc = row['document']

        for field in FIELDS:
            row[field] = doc[field][0]
        for field in OPTIONAL_FIELDS:
            row[field] = doc[field][0] if doc.get(field) else None

        num_stakeholders = len(doc[TABLE_FIELDS[0]])
        stakeholder_fields = {k:doc[k] for k in TABLE_FIELDS }
        stakeholder_fields.update({k: doc[k] if doc.get(k) else [None] * num_stakeholders  for k in OPTIONAL_TABLE_FIELDS})

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
        update_resource(
            -1, name='maya_stakeholder_list', path="data/maya_stakeholder_list.csv",
        ),
        filter_by_type,
        filter_corrupt_rows,
        rename_fields(RENAME_FIELDS),
        add_field('stakeholder_type', 'string'),
        add_fields(FIELDS + OPTIONAL_FIELDS, 'string'),
        add_fields(TABLE_FIELDS + OPTIONAL_TABLE_FIELDS,'string'),
        add_fields(CORE_STAKE_HOLDER_FIELDS.values(),'string'),
        validate,
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

