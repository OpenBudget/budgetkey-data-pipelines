import os
import json
from datapackage_pipelines.utilities.resources import PATH_PLACEHOLDER

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
    def history_steps(cls, resource_name, primary_key, fields, history_key=None):
        assert len(set(primary_key).intersection(set(fields))) == 0
        if history_key is None:
            history_key = '_'.join(sorted(fields))
        db_table = 'history_{}_{}'.format(resource_name, history_key).replace('-', '_')
        target_resource_name = db_table
        return steps(*[
            ('duplicate', {
                'source': resource_name,
                'target-name': target_resource_name,
                'target-path': PATH_PLACEHOLDER
            }),
            ('concatenate', {
                'target': {
                    'name': target_resource_name,
                    'path': PATH_PLACEHOLDER
                },
                'sources': target_resource_name,
                'fields': dict((f, []) for f in primary_key + fields) 
            }),
            ('add_timestamp', {
                'resource': target_resource_name
            }),
            ('join', {
                'source': {
                    'name': target_resource_name,
                    'key': primary_key + ['__updated_timestamp'],
                    'delete': True
                },
                'target': {
                    'name': target_resource_name,
                    'key': None
                },
                'fields': dict(
                    (f, {
                        'aggregate': 'last'
                    } if f in fields else None)
                    for f in primary_key + ['__updated_timestamp'] + fields
                )
            }), 
            ('filter_updated_items', {
                'db_table': db_table,
                'resource': target_resource_name,
                'key_fields': primary_key,
                'value_fields': fields
            }),
            ('set_primary_key', {
                target_resource_name: primary_key + ['__updated_timestamp']
            }),
            ('dump.to_sql', {
                'tables': {
                    db_table: {
                        'resource-name': target_resource_name,
                        'mode': 'update'
                    }
                }
            }),
            ('drop_resource', {
                'resource': target_resource_name
            })
        ])

    @classmethod
    def generate_pipeline(cls, source, base):
        all_pipelines = []
        sitemap_params = []
        bumper = source.get('bumper', 0)
        for doc_type, parameters in source.items():
            if not isinstance(parameters, dict):
                continue
            if not 'kind' in parameters:
                continue
            if parameters['kind'] == 'indexer':
                snake_doc_type = doc_type.replace('-', '_')
                dependent_pipeline_id = parameters['dependent_pipeline']
                source_datapackage = parameters['source_datapackage']
                if os.environ.get("ES_LOAD_FROM_URL") == "1":
                    # this allows to populate elasticsearch data without running dependant pipelines
                    source_datapackage = source_datapackage.replace("/var/datapackages", "http://next.obudget.org/datapackages")
                key_fields = parameters.get('key-fields', [])
                page_title_pattern = parameters.get('page-title-pattern')
                key_pattern = '/'.join([doc_type] + ['{%s}' % f for f in key_fields])
                key_pattern = parameters.get('key-pattern', key_pattern)
                pipeline_id = os.path.join(base, 'index_{}'.format(snake_doc_type))
                db_table = '_elasticsearch_mirror__{}'.format(snake_doc_type)
                revision = parameters.get('revision', 0) + bumper

                if doc_type != 'people':
                    all_pipelines.append(pipeline_id)
                    sitemap_params.append({
                        'kind': doc_type,
                        'db-table': db_table,
                        'doc-id': key_pattern
                    })

                keep_history = parameters.get('keep-history', [])
                history_steps = []
                for kh in keep_history:
                    history_steps.extend(
                        cls.history_steps(doc_type, key_fields, kh['fields'], kh.get('key'))
                    )
                date_range_parameters = parameters.get('date-range', {})

                pipeline_steps = steps(*[
                    ('update_package', {
                        'name': pipeline_id,
                    }),
                    ('load_big', {
                        'from': source_datapackage,
                        'resource': doc_type,
                    })]) + parameters.get('extra-steps', []) + steps(*[
                    ('set-revision', {'revision': revision}),
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
                    ('set-revisions', {}),
                    ('filter', {
                        'resources': doc_type,
                        'in': [
                            {'__next_update_days': 1},
                            # {'__next_update_days': 2},
                        ]
                    }),
                ]) + history_steps + steps(*[
                    ('add_doc_id', {
                        'doc-id-pattern': key_pattern
                    }),
                    ('add_page_title', {
                        'page-title-pattern': page_title_pattern
                    }),
                    ('add_date_range', date_range_parameters),
                    ('dump_to_es', {
                        'indexes': {
                            'budgetkey': [
                                {'resource-name': doc_type,
                                 'doc-type': doc_type,
                                 'revision': revision}
                            ]
                        }
                    }),
                    ('dpdumper', {
                        'out-path': '/var/datapackages/budgetkey/{}'.format(doc_type)
                    })                    
                ]) + parameters.get('document-steps', []) + steps(*[                   
                    ('convert_to_key_value'
                    ),
                    ('dump_to_es', {
                        'indexes': {
                            'budgetkey': [
                                {'resource-name': 'document',
                                 'doc-type': 'document'}
                            ]
                        }
                    }),
                ])

                if os.environ.get("ES_LIMIT_ROWS"):
                    dump_to_sql_indices = [i for i, s in enumerate(pipeline_steps) if s.get("run") == "dump.to_sql"]
                    assert len(dump_to_sql_indices) > 0
                    pipeline_steps.insert(
                        dump_to_sql_indices[0],
                        {"run": "limit_rows", "parameters": {"stop-after-rows": int(os.environ.get("ES_LIMIT_ROWS"))}}
                    )

                pipeline = {
                    'dependencies': [
                        {'pipeline': dependent_pipeline_id}
                    ],
                    'pipeline': pipeline_steps
                }
                if os.environ.get("ES_LOAD_FROM_URL") == "1":
                    del pipeline["dependencies"]
                yield pipeline_id, pipeline
        
        sitemaps_pipeline = {
            'dependencies': [
                {'pipeline': pipeline_id}
                for pipeline_id in all_pipelines
            ],
            'pipeline': steps(*[
                ('build_sitemaps', params)
                for params in sitemap_params
            ] + [
                ('build_sitemaps_index', {})
            ])
        }
        yield os.path.join(base, 'sitemaps'), sitemaps_pipeline
