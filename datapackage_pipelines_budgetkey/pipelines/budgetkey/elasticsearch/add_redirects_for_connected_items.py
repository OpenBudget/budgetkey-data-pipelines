from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

def process_resource(res_):
    for row in res_:
        yield row
        for year, rec in row.get('history', {}).items():
            for code_title in rec.get('code_titles', []):
                code, tite = code_title.split(':')
                doc_id = 'budget/{}/{}'.format(code, year)
                yield dict(
                    code=code, 
                    year=int(year),
                    __redirect=doc_id
                )


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


dp['resources'][0]['schema']['fields'].append(
    {
        'name': '__redirect',
        'type': 'string',
    }
)

spew(dp, process_resources(res_iter))
