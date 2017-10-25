import re

from datapackage_pipelines.wrapper import spew, ingest

parameters, datapackage, res_iter = ingest()


def process_first(rows):
    for row in rows:
        if any(v for k, v in row.items() if k.endswith('_revised')):
            yield row
        else:
            for k, v in row.items():
                if k.endswith('_allocated'):
                    row[k.replace('_allocated', '_revised')] = v
            yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_first(first)

    for res in res_iter_:
        yield res

spew(datapackage, process_resources(res_iter))
