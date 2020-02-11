from datapackage_pipelines.wrapper import ingest, spew
import json
import logging


parameters, datapackage, res_iter = ingest()


def process_resource(resource):
    for row in resource:
        if row['details']:
            details = row['details'][0]
            for d in row['details'][1:]:
                details.update(d)
            row['details'] = details
        yield row


def process_resources(res_iter_):
    for resource in res_iter_:
        yield process_resource(resource)


def modify_datapackage(datapackage):
    if 'detailsSchema' in datapackage:
        ds = datapackage.pop('detailsSchema')
        for resource in datapackage['resources']:
            for field in resource['schema']['fields']:
                if field['name'] == 'details':
                    field['es:schema'] = ds
                    logging.info('DETAILS SCHEMA\n%s', json.dumps(ds, indent=2, ensure_ascii=False))
                    break
    return datapackage


spew(modify_datapackage(datapackage), 
     process_resources(res_iter))