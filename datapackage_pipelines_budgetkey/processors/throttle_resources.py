from datapackage_pipelines.wrapper import spew, ingest
import time, logging, datetime, sys


def filter_resource(resource, sleep_seconds):
    yield from resource
    time.sleep(sleep_seconds)


def filter_resources(datapackage, resources, parameters):
    input_resource_name = parameters.get("resource")
    sleep_seconds = float(parameters.get("sleep-seconds", 2))  # sleep 2 seconds between resources
    for resource_descriptor, resource in zip(datapackage["resources"], resources):
        if not input_resource_name or input_resource_name == resource_descriptor["name"]:
            logging.info("throttling resource {}: sleep_seconds={}".format(resource_descriptor["name"],
                                                                           sleep_seconds))
            yield filter_resource(resource, sleep_seconds)
        else:
            yield resource


parameters, datapackage, resources = ingest()
spew(datapackage, filter_resources(datapackage, resources, parameters))
