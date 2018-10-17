from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from dataflows import Flow, add_computed_field


def update_row(key_pattern):
    def func(row):
        row['doc_id'] = key_pattern.format(**row)
    return func


def flow(parameters):
    key_pattern = parameters['doc-id-pattern']

    return Flow(
        add_computed_field([{
            'target': 'doc_id',
            'operation': 'constant',
            'with': ''
        }]),
        update_row(key_pattern)
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
