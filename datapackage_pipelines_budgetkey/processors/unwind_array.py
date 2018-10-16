import logging

from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher


def process_resource(res, afield, tfield):
    for row in res:
        array = row.get(afield, [])
        if array:
            del row[afield]
            for x in array:
                row[tfield] = x
                yield row


def process_resources(res_iter, resource_matcher, afield, tfield):
    for res in res_iter:
        if resource_matcher.match(res.spec['name']):
            yield process_resource(res, afield, tfield)
        else:
            yield res


def modify_datapackage(dp, resource_matcher, afield, tfield):
    for res in dp['resources']:
        if not resource_matcher.match(res['name']):
            continue
        field = [
            f for f in res['schema']['fields']
            if f['name'] == afield
        ][0]
        fields = [
            f for f in res['schema']['fields']
            if f['name'] != afield
        ]
        fields.append({
            'name': tfield,
            'type': field['es:itemType'] if 'es:itemType' in field else 'string'
        })
        res['schema']['fields'] = fields
    return dp


if __name__ == '__main__':
    parameters, dp, res_iter = ingest()
    resource_matcher = ResourceMatcher(parameters.get('resource'))
    afield, tfield = parameters['array-field'], parameters['unwound-field']
    spew(modify_datapackage(dp, resource_matcher, afield, tfield),
         process_resources(res_iter, resource_matcher,
                           afield, tfield))