from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.lib.load import flow as load_flow
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver


def flow(parameters):
    gcl = google_chrome_driver()
    parameters['from'] = gcl.download(parameters['from'])
    gcl.teardown()    
    return load_flow(parameters)


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
