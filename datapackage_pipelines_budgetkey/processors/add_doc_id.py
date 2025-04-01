
import logging

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines_budgetkey.common.short_doc_id import calc_short_doc_id

from dataflows import Flow, add_field


def get_doc_id(key_pattern):
    def func(row):
        ret = key_pattern.format(**row)
        if len(ret.encode('utf8')) > 400:
            logging.warning('DOC_ID length is %d %s (%r)', len(ret), ret, row)
        return ret
    return func

def get_item_url():
    def func(row):
        doc_id = row['doc_id']
        url, _ = calc_short_doc_id(doc_id)
        return url
    return func

def flow(parameters):
    key_pattern = parameters['doc-id-pattern']

    return Flow(
        add_field('doc_id', 'string', get_doc_id(key_pattern), **{'es:index': False}),
        add_field('item-url', 'string', get_item_url(), **{'es:index': False}),
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
