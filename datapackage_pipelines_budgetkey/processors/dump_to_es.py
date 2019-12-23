from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from dataflows_elasticsearch import dump_to_es
from tableschema_elasticsearch.mappers import MappingGenerator
import dataflows as DF

import logging
import collections
import datetime


def id(x):
    return x


class BoostingMappingGenerator(MappingGenerator):

    def __init__(self):
        super(BoostingMappingGenerator, self).__init__(base={})

    @classmethod
    def _convert_type(cls, schema_type, field, prefix):
        prop = super(BoostingMappingGenerator, cls)._convert_type(schema_type, field, prefix)
        if field.get('es:keyword'):
            prop['type'] = 'keyword'
            if field.get('es:title'):
                prop['boost'] = 100
        elif schema_type == 'string':
            if field.get('es:title'):
                prop['boost'] = 100
            if field.get('es:title') or field.get('es:hebrew'):
                prop['fields'] = {
                    "hebrew": {
                    "type": "text",
                    'analyzer': 'hebrew'
                }
            }
        elif schema_type in ('number', 'integer','datetime'):
            prop['index'] = True
        return prop


class DumpToElasticSearch(dump_to_es):

    def __init__(self, indexes):
        super().__init__(
            indexes=indexes,
            mapper_cls=BoostingMappingGenerator,
            index_settings={
                'index.mapping.coerce': True
            }
          )

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
        for index_name, configs in self.index_to_resource.items():
            for config in configs:
                if 'revision' in config:
                    revision = config['revision']
                    logging.info('DELETING from "%s", iitems with revision < %d',
                                index_name, revision)
                    ret = self.engine.delete_by_query(
                        index_name,
                        {
                            "query": {
                                "range": {
                                    "__revision": {
                                        "lt": revision
                                    }
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
