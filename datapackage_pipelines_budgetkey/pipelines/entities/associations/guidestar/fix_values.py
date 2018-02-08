from datapackage_pipelines.wrapper import process
from datapackage import Package

from fuzzywuzzy import process as fw_process

lamas_data = Package('/var/datapackages/lamas-municipal-data/datapackage.json')

districts = dict(
    (x['entity_name'], x['district_2015'])
    for x in lamas_data.resources[0].iter(keyed=True)
)


def process_row(row, *_):
    association_activity_region = row.get('association_activity_region')
    if association_activity_region is not None:
        row['association_activity_region_list'] = [x.strip() for x in association_activity_region.split(',')]
    else:
        row['association_activity_region_list'] = []
    association_activity_region_districts = set()
    for city in row['association_activity_region_list']:
        best = fw_process.extract(city, districts.keys())
        association_activity_region_districts.add(best)
    row['association_activity_region_districts'] = list(association_activity_region_districts)

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
        }
    ])
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)