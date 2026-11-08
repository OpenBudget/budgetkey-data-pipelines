"""dpp flow: unions the per-source outputs into the related_items and related_items_mentions tables.

Parameters: sources (datapackage dirs written by link_source), out-path.
"""
import os

import dataflows as DF

from datapackage_pipelines_budgetkey.pipelines.related_items.linker import LINK_FIELDS, MENTION_FIELDS


def flow(parameters, *_):
    steps = []
    for path in parameters['sources']:
        steps.append(DF.load(os.path.join(path, 'datapackage.json'),
                             resources=['related_items', 'related_items_mentions']))
    for name, fields in (('related_items', LINK_FIELDS), ('related_items_mentions', MENTION_FIELDS)):
        steps.append(DF.concatenate({f: [] for f, _ in fields}, target=dict(name=name, path=name + '.csv'),
                                    resources=name))
        steps.extend(DF.set_type(f, type=t, resources=name) for f, t in fields)
    steps.append(DF.set_primary_key(['link_id'], resources='related_items'))
    steps.append(DF.update_resource(None, **{'dpp:streaming': True}))
    steps.append(DF.dump_to_path(parameters['out-path']))
    if parameters.get('dump-to-sql', True):
        from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic
        steps.append(dump_to_sql_atomic(dict(
            related_items={'resource-name': 'related_items'},
            related_items_mentions={'resource-name': 'related_items_mentions'},
        )))
    return DF.Flow(*steps)
