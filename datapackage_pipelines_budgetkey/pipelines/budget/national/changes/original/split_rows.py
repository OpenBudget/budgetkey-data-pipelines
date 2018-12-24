import copy

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from dataflows import Flow, add_field


def split_rows(rows):
    for row in rows:
        for i in range(4):
            r = copy.deepcopy(row)
            r['code'] = r['code'][:i*2+2]
            yield r


def flow(_):
    return Flow(
        split_rows
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
