from datapackage_pipelines_elasticsearch.processors.dump.to_index import ESDumper
from tableschema_elasticsearch.mappers import MappingGenerator

import logging
import collections
import datetime


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
        elif schema_type in ('number', 'integer'):
            prop['index'] = True
        return prop



class DumpToElasticSearch(ESDumper):
    
    def __init__(self):
        super(DumpToElasticSearch, self).__init__(
            mapper_cls=BoostingMappingGenerator,
            index_settings={
                'index.mapping.coerce': True
            }
          )
    
    def format_datetime_rows(self, spec, rows):
        formatters = {}
        for f in spec['schema']['fields']:
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
        id = lambda x: x

        for row in rows:
            yield dict((k, formatters.get(k, id)(v)) for k, v in row.items())

    def initialize(self, parameters):
        parameters['reindex'] = False
        return super(DumpToElasticSearch, self).initialize(parameters)

    def handle_resource(self, resource, spec, parameters, datapackage):
        try:
            return super(DumpToElasticSearch, self)\
                    .handle_resource(self.format_datetime_rows(spec, resource), 
                                    spec, parameters, datapackage)
        except Exception:
            logging.exception('DUMP TO ES ERROR')
            raise

    # def __call__(self):
    #     super(DumpToElasticSearch, self).__call__()
    #     self.finalize()

    def finalize(self):
        for index_name, configs in self.index_to_resource.items():
            for config in configs:
                if 'revision' in config:
                    revision = config['revision']
                    doc_type = config['doc-type']
                    logging.info('DELETING from "%s", "%s" items with revision < %d',
                                index_name, doc_type, revision)
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
                        doc_type=doc_type
                    )
                    logging.info('GOT %r', ret)


if __name__ == '__main__':
    DumpToElasticSearch()()