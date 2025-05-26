from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.utilities.extended_json import LazyJsonLine
from dataflows_elasticsearch import dump_to_es
from tableschema_elasticsearch.mappers import MappingGenerator
import dataflows as DF

from elasticsearch import Elasticsearch
import elastic_transport
import logging
import os
import json
import time


def id(x):
    return x


class BoostingMappingGenerator(MappingGenerator):

    def __init__(self):
        super(BoostingMappingGenerator, self).__init__(base={})

    @classmethod
    def _convert_type(cls, schema_type, field, prefix):
        if field['name'] == 'chunks':
            return {
                'type': 'nested', 
                'properties': {
                    'embeddings': {
                        'type': 'dense_vector',
                        'dims': 1536,
                        'index': True,
                        'similarity': 'cosine'
                    }
                }
            }
        prop = super(BoostingMappingGenerator, cls)._convert_type(schema_type, field, prefix)
        if field.get('es:keyword'):
            prop['type'] = 'keyword'
        # elif schema_type == 'string':
        #     if field.get('es:title') or field.get('es:hebrew'):
        #         prop['fields'] = {
        #             "hebrew": {
        #             "type": "text",
        #             'analyzer': 'hebrew'
        #         }
        #     }
        elif schema_type in ('number', 'integer', 'datetime'):
            prop['index'] = True
        return prop

    def generate_from_schema(self, schema):
        super(BoostingMappingGenerator, self).generate_from_schema(schema)
        logging.info('GENERATED MAPPING: %s', json.dumps(self.get_mapping(), indent=2, ensure_ascii=False))

class MyNode(elastic_transport.RequestsHttpNode):

    def perform_request(self, method, *args, **kwargs):
        headers = kwargs.get('headers') or {}
        if method == 'HEAD':
            method = 'GET'
            print('HEAD request, changing to GET')
            headers['Connection'] = 'close'
        kwargs['headers'] = headers
        return super().perform_request(method, *args, **kwargs)

class DumpToElasticSearch(dump_to_es):

    def __init__(self, indexes):
        engine = Elasticsearch(
            os.environ['DATAFLOWS_ELASTICSEARCH'],
            basic_auth=('elastic', os.environ['ELASTIC_PASSWORD']),
            ca_certs=os.environ['ELASTICSEARCH_CA_CRT'], request_timeout=300,
            node_class=MyNode
        )
        print(f'Elasticsearch: {os.environ["DATAFLOWS_ELASTICSEARCH"]}, auth: {os.environ["ELASTIC_PASSWORD"][:2]}..{os.environ["ELASTIC_PASSWORD"][-2:]}, ca_certs: {os.environ["ELASTICSEARCH_CA_CRT"]}')
        assert engine.ping(), 'Elasticsearch is not reachable'
        super().__init__(
            engine=engine,
            indexes=indexes,
            mapper_cls=BoostingMappingGenerator,
            index_settings={
                'index.mapping.coerce': True
            }
          )

    def normalize(self, row, resource=None):
        if isinstance(row, LazyJsonLine):
            row = row._evaluate()
        return super().normalize(row, resource)

    def normalizer(self, resource: DF.ResourceWrapper):
        formatters = {}
        for f in resource.res.descriptor['schema']['fields']:
            if f['type'] == 'datetime':
                logging.info('FIELD datetime: %r', f)
                if f.get('format', 'default') in ('any', 'default'):
                    formatters[f['name']] = lambda x: None if x is None else x.strftime('%Y-%m-%dT%H:%M:%SZ')
                else:
                    def formatter(f):
                        fmt = f['format']

                        def func(x):
                            if x is None:
                                return None
                            else:
                                return x.strftime(fmt)
                        return func

                    formatters[f['name']] = formatter(f)

        for row in super().normalizer(resource):
            yield dict((k, formatters.get(k, id)(v)) for k, v in row.items())

    def finalize(self):
        time.sleep(10)
        for index_name, configs in self.index_to_resource.items():
            for config in configs:
                if 'revision' in config:
                    revision = config['revision']
                    logging.info('DELETING from "%s", items with revision < %d',
                                index_name, revision)
                    ret = self.engine.delete_by_query(
                        index=index_name,
                        query={
                            "range": {
                                "__revision": {
                                    "lt": revision
                                }
                            }
                        },
                    )
                    logging.info('GOT %r', ret)


def flow(parameters):
    return DF.Flow(
        DumpToElasticSearch(parameters['indexes'])
    )

if __name__ == '__main__':
    try:
        with ingest() as ctx:
            spew_flow(flow(ctx.parameters), ctx)
    except Exception as e:
        logging.error('DUMP TO ES ERROR %s', str(e)[:64000])
        if hasattr(e, 'errors'):
            for error in e.errors:
                logging.error('error: %s', str(error)[:64000])
        logging.exception('TB')
        raise
