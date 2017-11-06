import re

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()


def process_resource(res_):
    for row in res_:
        semel = row.get('symbol_municipality_2015')
        try:
            # Make sure this is a valid row
            int(semel)
            for k, v in row.items():
                if v in ['..', '-']:
                    row[k] = ''
                if isinstance(v, str) and len(v) > 1:
                    if v[0] == '(' and v[-1] == ')':
                        v = row[k] = ''
                    if '.000' in v:
                        v = row[k] = v.replace('.000', '')
                    if len(re.findall('[.]', v)) > 1:
                        v = row[k] = ''

            yield row
        except (ValueError, TypeError):
            continue


def process_resources(res_iter_):
    for res in res_iter_:
        if 'lamas' in res.spec['name']:
            yield process_resource(res)
        else:
            yield res

spew(datapackage, process_resources(res_iter))