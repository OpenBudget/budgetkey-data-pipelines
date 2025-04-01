from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_budgetkey.common.short_doc_id import calc_short_doc_id

parameters, dp, res_iter = ingest()

def process_resource(res_):
    for row in res_:
        yield row
        for year, rec in row.get('history', {}).items():
            for code_title in rec.get('code_titles', []):
                code, title = code_title.split(':', 1)
                doc_id = 'budget/{}/{}'.format(code, year)
                short_url, _ = calc_short_doc_id(doc_id)
                ret = dict(
                    code=code,
                    title=title,
                    year=int(year),
                    doc_id=doc_id,
                    __redirect=row['doc_id']
                )
                ret['item-url'] = short_url
                yield ret


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
