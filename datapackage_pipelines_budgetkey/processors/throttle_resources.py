from datapackage_pipelines.wrapper import spew, ingest
import time, logging, datetime, sys


def filter_resources(resources, parameters):
    input_resource_name = parameters.get("resource")
    sleep_seconds = float(parameters.get("sleep-seconds", 2))  # sleep 2 seconds between resources
    for resource in resources:
        yield resource
        if not input_resource_name or input_resource_name == resource.spec["name"]:
            logging.info("throttling resource {}: sleep_seconds={}".format(resource.spec["name"],
                                                                           sleep_seconds))
            time.sleep(sleep_seconds)


parameters, datapackage, resources = ingest()
spew(datapackage, filter_resources(resources, parameters))
