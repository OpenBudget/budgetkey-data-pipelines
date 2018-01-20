from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()


def process_resource(resource):
    for row in resource:
        if row['details']:
            details = row['details'][0]
            for d in row['details'][1:]:
                details.update(d)
            row['details'] = details
        yield row


def process_resources(res_iter_):
    for resource in res_iter_:
        yield process_resource(resource)


spew(datapackage, process_resources(res_iter))