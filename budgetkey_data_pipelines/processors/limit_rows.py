from datapackage_pipelines.wrapper import spew, ingest


def filter_resource(resource, parameters):
    stop_after_rows = int(parameters["stop-after-rows"])
    for i, row in enumerate(resource):
        if i < stop_after_rows:
            yield row

def filter_resources(datapackage, resources, parameters):
    for resource_descriptor, resource in zip(datapackage["resources"], resources):
        yield filter_resource(resource, parameters)


parameters, datapackage, resources = ingest()
spew(datapackage, filter_resources(datapackage, resources, parameters))
