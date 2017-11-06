import datetime
import re

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew
from decimal import Decimal

parameters, datapackage, res_iter = ingest()

all_entity_ids = set()


def process_resource(resource):
    for row in resource:
        if row['id'] not in all_entity_ids:
            yield row
            all_entity_ids.add(row['id'])


def process_resources(res_iter_):
    for resource in res_iter_:
        yield process_resource(resource)


spew(datapackage, process_resources(res_iter))