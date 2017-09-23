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
                key_fields = parameters['key-fields']
                pipeline_id = 'index_{}'.format(snake_doc_type)
                db_table = '_elasticsearch_mirror__{}'.format(snake_doc_type)

                pipeline_steps = [
                    ('add_metadata', {
                        'name': pipeline_id,
                        'title': 'index_of_{}'.format(doc_type)
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
                        'key-prefix': doc_type,
                        'key-fields': key_fields
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

                pipeline = {
                    'dependencies': [
                        {'pipeline': dependent_pipeline_id}
                    ],
                    'pipeline': steps(*pipeline_steps)
                }
                yield pipeline_id, pipeline