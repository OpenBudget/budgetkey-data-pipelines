from datapackage_pipelines.wrapper import process
from datapackage import Package
import json
import logging
import datetime
import tabulator

from fuzzywuzzy import process as fw_process

logging.info('LOADING LAMAS DATA')
lamas_data = Package('/var/datapackages/lamas-municipal-data/datapackage.json')
districts = dict(
    (x['entity_name'], x['district_2015'])
    for x in lamas_data.resources[0].iter(keyed=True)
)

logging.info('LOADING FOA IMPROVEMENT')
foa_improvement = tabulator.Stream('https://docs.google.com/spreadsheets/d/17kX25p_M59h6VoDB90BeNVSmrel_9ipxUyko8SD6eKY/edit?usp=sharing', headers=1)
foa_improvement = foa_improvement.open().iter(keyed=True)
foa_improvement = dict((x.pop('original'), x) for x in foa_improvement)


regional_towns = Package('/var/datapackages/lamas-municipality-locality-map/datapackage.json')
cache = {}
for town, municipality in regional_towns.resources[0].iter():
    district = None
    if municipality in cache:
        district = cache[municipality]
    else:
        best = fw_process.extract(municipality, districts.keys())
        if len(best)>0:
            score = best[0][1]
            best = best[0][0]
            district = districts[best]
            cache[municipality] = district
    if district is not None:
        districts[town] = district
logging.info('LOADING LAMAS DONE')

primary_categories = json.load(open('activity_areas.json'))
primary_categories = dict(
    (x['Secondary_Text__c'], x['Primary_Text__c'])
    for x in primary_categories[0]['result']
    if 'Secondary_Text__c' in x
)

min_activity_year = datetime.datetime.now().year - 3

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
        district = None
        if city in cache:
            district = cache[city]
        else:
            best = fw_process.extract(city, districts.keys())
            if len(best)>0:
                best = best[0][0]
                district = districts[best]
                cache[city] = district
        if district is not None:
            association_activity_region_districts.add(district)
    row['association_activity_region_districts'] = list(association_activity_region_districts)

    if row['association_field_of_activity']:
        foa = row['association_field_of_activity']
        foa = FIELD_FIXES.get(foa, foa)
        try:
            row['association_primary_field_of_activity'] = primary_categories[foa]
        except:
            logging.error('unknown primary field of activity for foa "%s" %s', foa, row_index)
            row['association_primary_field_of_activity'] = 'unknown'
        prefix = 'אחר - '
        if foa.startswith(prefix):
            foa = foa[len(prefix):]
        prefix = 'תחום אחר - '
        if foa.startswith(prefix):
            foa = foa[len(prefix):]
        foa = foa_improvement[foa]
        row['association_field_of_activity'] = foa['improved']
        row['association_field_of_activity_display'] = foa['display']
    else:
        row['association_primary_field_of_activity'] = ''
        row['association_field_of_activity_display'] = 'לא ידוע'

    row['association_status_active'] = any(row.get('association_' + x) is not None and row.get('association_' + x) >= min_activity_year 
                                           for x in ('last_report_year', 'online_data_update_year'))
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
        },
        {
            'name': 'association_status_active',
            'type': 'boolean',
        },
        {
            'name': 'association_field_of_activity_display',
            'type': 'string',
        },
    ])
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)