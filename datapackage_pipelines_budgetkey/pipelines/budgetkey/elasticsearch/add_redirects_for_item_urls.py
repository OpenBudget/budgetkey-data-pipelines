from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines_budgetkey.common.short_doc_id import calc_short_doc_id

parameters, dp, res_iter = ingest()


def process_resource(res_):
    for row in res_:
        yield row
        doc_id = row['doc_id']
        _, short_item_id = calc_short_doc_id(doc_id)
        yield dict(
            doc_id=short_item_id,
            __redirect=doc_id
        )


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


if '__redirect' not in [f['name'] for f in dp['resources'][0]['schema']['fields']]:
    dp['resources'][0]['schema']['fields'].append(
        {
            'name': '__redirect',
            'type': 'string',
        }
    )

spew(dp, process_resources(res_iter))
