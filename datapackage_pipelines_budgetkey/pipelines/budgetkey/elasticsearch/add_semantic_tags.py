import tabulator

from datapackage_pipelines.wrapper import process
import re

tag_areas = tabulator.Stream('https://docs.google.com/spreadsheets/d/1k4iSVsX79-VMZCYoHFRL5Q6GAMP6M1t2hIFv7k4E8eM/edit#gid=0',
                        headers=1)
tag_areas.open()
tag_areas = list(tag_areas.iter())
tag_areas = [[x.strip() for x in y[0].split(',')] for y in tag_areas]
tag_res = [re.compile(r'\b(' + 
                      '|'.join('({})'.format(tag) for tag in tag_area) + 
                      r')\b')
           for tag_area in tag_areas]
tag_areas = list(zip(tag_res, tag_areas))


def process_row(row, _1, spec, _2, params, _3):
    resource = params['resource']
    target_field = params['target-field']
    source_fields = params['source-fields']
    if spec['name'] == resource:
        tags = set()
        for field in source_fields:
            value = row[field]
            if value:
                for tag_re, tag_area in tag_areas:
                    if tag_re.search(value):
                        tags.update(tag_area)
                        break
        row[target_field] = list(sorted(tags))
    return row


def modify_datapackage(dp, params, *_):
    resource = params['resource']
    target_field = params['target-field']
    for res in dp['resources']:
        if res['name'] == resource:
            res['schema']['fields'].append({
                'name': target_field,
                'type': 'array',
                'es:itemType': 'string',
            })


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
