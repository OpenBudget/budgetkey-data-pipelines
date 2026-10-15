import csv

from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_budgetkey.processors.fingerprint import calc_fingerprint

parameters, datapackage, res_iter = ingest()

# Authorities renamed since the 2015 CBS file: the entity registry knows them only by the new name.
# Only the fingerprint used to find the entity changes; name_municipality keeps the CBS name, which
# other consumers (e.g. datacity) match budget rows against.
RENAMED = dict(
    (x['from'], x['to']) for x in
    csv.DictReader(open('renamed_authorities.csv'))
)


def process_resource(res_):
    for row in res_:
        if row['name_municipality'] in RENAMED:
            row['fingerprint'] = calc_fingerprint(RENAMED[row['name_municipality']])
        yield row


def process_resources(res_iter_):
    for res in res_iter_:
        if res.spec['name'] == parameters['resource-name']:
            yield process_resource(res)
        else:
            yield res


spew(datapackage, process_resources(res_iter))
