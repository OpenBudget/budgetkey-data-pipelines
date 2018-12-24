from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.lib.load import flow as load_flow
import csv


def flow(parameters):
    csv.field_size_limit(2*1024*1024)
    return load_flow(parameters)


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
