from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, *_):
    for res in dp['resources']:
        for k, v in res.items():
            if k.startswith('collated-schema:'):
                collated_field = k[len('collated-schema:'):]
                for f in res['schema']['fields']:
                    if f['name'] == collated_field:
                        assert f['type'] == 'array'
                        f['es:schema'] = v
                        f['es:itemType'] = 'object'
                        break
                del res[k]
                break
    return dp

process(modify_datapackage=modify_datapackage)