from datapackage_pipelines.wrapper import spew, ingest
import time, logging, datetime, sys


def filter_resource(resource, sleep_seconds, start_time, log_interval_seconds):
    last_log_seconds = 0
    num_processed_rows = 0
    for row in resource:
        yield row
        sys.stdout.flush()
        num_processed_rows += 1
        elapsed_seconds = (datetime.datetime.now()-start_time).total_seconds()
        seconds_since_last_log = elapsed_seconds - last_log_seconds
        if seconds_since_last_log > log_interval_seconds:
            last_log_seconds = elapsed_seconds
            logging.info("processed {} rows, elapsed time (seconds)={}".format(num_processed_rows,
                                                                               elapsed_seconds))
        time.sleep(sleep_seconds)


def filter_resources(datapackage, resources, parameters):
    input_resource_name = parameters.get("resource")
    sleep_seconds = float(parameters.get("sleep-seconds", 2))  # sleep 2 seconds between rows
    log_interval_seconds = int(parameters.get("log-interval-seconds", 60))  # log every 60 seconds
    start_time = datetime.datetime.now()
    for resource_descriptor, resource in zip(datapackage["resources"], resources):
        if not input_resource_name or input_resource_name == resource_descriptor["name"]:
            logging.info("throttling resource {}: sleep_seconds={}".format(resource_descriptor["name"],
                                                                           sleep_seconds))
            yield filter_resource(resource, sleep_seconds, start_time, log_interval_seconds)
        else:
            yield resource


parameters, datapackage, resources = ingest()
spew(datapackage, filter_resources(datapackage, resources, parameters))
