from dataflows import Flow
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from datapackage_pipelines_budgetkey.processors.data_gov_il_resource import flow
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver, finalize_teardown


def batch_flow(parameters):
    gcd = google_chrome_driver()
    finalize_teardown(gcd)
    return Flow(
        *[flow(dict(**p, gcd=gcd)) for p in parameters['batch']]
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(batch_flow(ctx.parameters), ctx)
