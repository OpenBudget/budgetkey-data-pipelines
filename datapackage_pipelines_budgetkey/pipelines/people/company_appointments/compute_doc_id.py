from datapackage_pipelines.wrapper import process
import logging

def process_row(row, *_):
    entity_id = row['entity_id']

    if entity_id:
        row['doc_id'] = "/org/{entity_kind}/{entity_id}".format(**row)

    del row['entity_id']
    del row['entity_kind']
    del row['entity_name']
    return row


def modify_datapackage(dp, *_):
    schema = dp['resources'][0]['schema']
    filtered_fields = [field for field in schema['fields'] if field['name'] not in ['entity_id', 'entity_kind', 'entity_name']]
    schema['fields'] = filtered_fields
    schema['fields'].extend([
        {'name': 'doc_id', 'type': 'string'}
    ])

    return dp

process(modify_datapackage=modify_datapackage, process_row=process_row)