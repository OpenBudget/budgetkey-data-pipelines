from datapackage_pipelines.wrapper import process
from datapackage import Package
import json
import logging

from fuzzywuzzy import process as fw_process

lamas_data = Package('/var/datapackages/lamas-municipal-data/datapackage.json')

districts = dict(
    (x['entity_name'], x['district_2015'])
    for x in lamas_data.resources[0].iter(keyed=True)
)

primary_categories = json.load(open('activity_areas.json'))
primary_categories = dict(
    (x['Secondary_Text__c'], x['Primary_Text__c'])
    for x in primary_categories[0]['result']
)


FIELD_FIXES = {
    'מחקר, מדע וטנכולוגיה':
        'מחקר ומדע',
    'מחקר, מדע וטכנולוגיה':
        'מחקר ומדע',
    'אחר - מחקר, מדע וטכנולוגיה':
        'מחקר ומדע',
    'שירותי רווחה':
        'אחר - שירותי רווחה',
    'מורשת או הנצחה':
        'אחר - מורשת והנצחה',
    'ספורט':
        'אחר - ספורט',
    'חינוך, השכלה והכשרה מקצועית':
        'אחר - חינוך, השכלה והכשרה מקצועית',
    'קהילה וחברה':
        'אחר - קהילה וחברה',
    'תרבות או אמנות':
        'אחר - תרבות ואומנות',
    'דת':
        'אחר - דת',
    'ארגוני סנגור, שינוי חברתי ופוליטי':
        'אחר - ארגוני סנגור, שינוי חברתי ופוליטי',    
}

def process_row(row, row_index, *_):
    association_activity_region = row.get('association_activity_region')
    if association_activity_region is not None:
        row['association_activity_region_list'] = [x.strip() for x in association_activity_region.split(',')]
    else:
        row['association_activity_region_list'] = []
    association_activity_region_districts = set()
    for city in row['association_activity_region_list']:
        best = fw_process.extract(city, districts.keys())
        if len(best)>0:
            best = best[0][0]
            district = districts[best]
            association_activity_region_districts.add(district)
    row['association_activity_region_districts'] = list(association_activity_region_districts)

    if row['association_field_of_activity']:
        foa = row['association_field_of_activity']
        foa = FIELD_FIXES.get(foa, foa)
        try:
            row['association_primary_field_of_activity'] = primary_categories[foa]
        except:
            logging.error('offending row "%s" %s %r', foa, row_index, row)
        prefix = 'אחר - '
        if foa.startswith(prefix):
            foa = foa[len(prefix):]
        row['association_field_of_activity'] = foa
    else:
        row['association_primary_field_of_activity'] = ''
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {
            'name': 'association_activity_region_list',
            'type': 'array',
            'es:itemType': 'string'
        },
        {
            'name': 'association_activity_region_districts',
            'type': 'array',
            'es:itemType': 'string'
        },
        {
            'name': 'association_primary_field_of_activity',
            'type': 'string',
        }
    ])
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)