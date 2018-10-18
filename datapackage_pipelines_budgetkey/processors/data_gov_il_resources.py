from dataflows import Flow
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from datapackage_pipelines_budgetkey.processors.data_gov_il_resource import flow


def batch_flow(parameters):
    return Flow(
        *[flow(p) for p in parameters['batch']]
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(batch_flow(ctx.parameters), ctx)
