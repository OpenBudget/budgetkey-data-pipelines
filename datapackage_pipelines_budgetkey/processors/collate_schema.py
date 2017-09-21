from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, *_):

    schemas = {}
    to_del = []
    for k, v in dp.items():
        if k.startswith('collated-schema:'):
            _, res_name, collated_field = k.split(':')
            schemas.setdefault(res_name, {})[collated_field] = v
            to_del.append(k)
    for k in to_del:
        del dp[k]

    for res in dp['resources']:
        if res['name'] not in schemas:
            continue
        res_schemas = schemas[res['name']]
        for f in res['schema']['fields']:
            if f['name'] in res_schemas:
                assert f['type'] == 'array'
                f['es:schema'] = res_schemas[f['name']]
                f['es:itemType'] = 'object'
    return dp

process(modify_datapackage=modify_datapackage)