from datapackage_pipelines.wrapper import spew, ingest

parameters, datapackage, res_iter = ingest()


def process_first(rows):
    for row in rows:
        if row.get('code') and len(row.get('code')) == 4:
            yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_first(first)

    for res in res_iter_:
        yield res


spew(datapackage, process_resources(res_iter))
