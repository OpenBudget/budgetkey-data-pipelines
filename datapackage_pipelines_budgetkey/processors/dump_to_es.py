from datapackage_pipelines_elasticsearch.processors.dump.to_index import ESDumper
from tableschema_elasticsearch.mappers import MappingGenerator

import logging
import collections


class BoostingMappingGenerator(MappingGenerator):

    def __init__(self):
        super(BoostingMappingGenerator, self).__init__(base={})

    @classmethod
    def _convert_type(cls, schema_type, field, prefix):
        prop = super(BoostingMappingGenerator, cls)._convert_type(schema_type, field, prefix)
        if schema_type == 'string':
            if field.get('es:title'):
                prop['boost'] = 100
            if field.get('es:title') or field.get('es:hebrew'):
                prop['fields'] = {
                    "hebrew": { 
                    "type": "text",
                    'analyzer': 'hebrew'
                }
          }
        return prop


class DumpToElasticSearch(ESDumper):
    
    def __init__(self):
        super(DumpToElasticSearch, self).__init__(
            mapper_cls=BoostingMappingGenerator,
            index_settings={
                'index.mapping.coerce': True
            }
        )

    def handle_resource(self, resource, spec, parameters, datapackage):
        def debugger(resource):
            resource_name = spec['name']
            seen = 0
            days_update = collections.Counter()
            debugged = None
            for row in resource:
                if row.get('key') == 'org/association/520028275' or row.get('id') == '520028275':
                    debugged = row
                    logging.error('%s: debug %r', resource_name, row)
                seen += 1
                days_update.update([row.get('__next_update_days')])
                yield row
            logging.error('%s: debug %r', resource_name, debugged)
            logging.error('%s: __next_update_days %r', resource_name, days_update.most_common())
            logging.error('%s: indexed %d rows', resource_name, seen)
        
        return super(DumpToElasticSearch, self).handle_resource(debugger(resource), spec, parameters, datapackage)


DumpToElasticSearch()()