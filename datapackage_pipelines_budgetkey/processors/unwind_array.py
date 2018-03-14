import logging

from datapackage_pipelines.wrapper import ingest, spew


def process_resource(res, afield, tfield):
    for row in res:
        array = row.get(afield, [])
        if array:
            del row[afield]
            for x in array:
                row[tfield] = x
                yield row


def process_resources(res_iter, afield, tfield):
    for res in res_iter:
        yield process_resource(res, afield, tfield)


def modify_datapackage(dp, afield, tfield):
    for res in dp['resources']:
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
    afield, tfield = parameters['array-field'], parameters['unwound-field']
    spew(modify_datapackage(dp, afield, tfield),
         process_resources(res_iter, 
                           afield, tfield))