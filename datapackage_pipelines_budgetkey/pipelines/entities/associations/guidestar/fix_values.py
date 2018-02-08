from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    association_activity_region = row.get('association_activity_region')
    if association_activity_region is not None:
        row['association_activity_region_list'] = [x.strip() for x in association_activity_region.split(',')]
    else:
        row['association_activity_region_list'] = []
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {
            'name': 'association_activity_region_list',
            'type': 'array',
            'es:itemType': 'string'
        }
    ])
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)