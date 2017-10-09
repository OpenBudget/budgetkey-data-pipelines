import os
import json

from datapackage_pipelines.generators import (
    GeneratorBase, steps
)

import logging


ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')
SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), 'schemas/budgetkey_spec_schema.json')


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        for doc_type, parameters in source.items():
            if parameters['kind'] == 'indexer':
                snake_doc_type = doc_type.replace('-', '_')
                dependent_pipeline_id = parameters['dependent_pipeline']
                source_datapackage = parameters['source_datapackage']
                if os.environ.get("ES_LOAD_FROM_URL") == "1":
                    # this allows to populate elasticsearch data without running dependant pipelines
                    source_datapackage = source_datapackage.replace("/var/datapackages", "http://next.obudget.org/datapackages")
                key_fields = parameters.get('key-fields', [])
                key_pattern = '/'.join([doc_type] + ['{%s}' % f for f in key_fields])
                key_pattern = parameters.get('key-pattern', key_pattern)
                pipeline_id = 'index_{}'.format(snake_doc_type)
                db_table = '_elasticsearch_mirror__{}'.format(snake_doc_type)

                pipeline_steps = [
                    ('add_metadata', {
                        'name': pipeline_id,
                    }),
                    ('load_resource', {
                        'url': source_datapackage,
                        'resource': doc_type,
                    }),
                    ('manage-revisions', {
                        'resource-name': doc_type,
                        'db-table': db_table,
                        'key-fields': key_fields
                    }),
                    ('dump.to_sql', {
                        'tables': {
                            db_table: {
                                'resource-name': doc_type,
                                'mode': 'update'
                            }
                        }
                    }),
                    ('filter', {
                        'resources': None,
                        'in': [
                            {'__next_update_days': 1}
                        ]
                    }),
                    ('dump_to_es', {
                        'indexes': {
                            'budgetkey': [
                                {'resource-name': doc_type,
                                 'doc-type': doc_type}
                            ]
                        }
                    }),
                    ('convert_to_key_value', {
                        'key-pattern': key_pattern
                    }),
                    ('sample'
                    ),
                    ('dump_to_es', {
                        'indexes': {
                            'budgetkey': [
                                {'resource-name': 'document',
                                 'doc-type': 'document'}
                            ]
                        }
                    })
                ]

                if os.environ.get("ES_LIMIT_ROWS"):
                    dump_to_sql_index = [i for i, s in enumerate(pipeline_steps) if s[0] == "dump.to_sql"][0]
                    pipeline_steps.insert(
                        dump_to_sql_index,
                        ("limit_rows", {"stop-after-rows": int(os.environ.get("ES_LIMIT_ROWS"))})
                    )

                pipeline = {
                    'dependencies': [
                        {'pipeline': dependent_pipeline_id}
                    ],
                    'pipeline': steps(*pipeline_steps)
                }
                if os.environ.get("ES_LOAD_FROM_URL") == "1":
                    del pipeline["dependencies"]
                yield pipeline_id, pipeline