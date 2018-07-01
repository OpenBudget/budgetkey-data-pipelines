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
        elif schema_type == 'integer':
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
    
    def format_date(self, v):
        DATE_FORMAT = '%Y-%m-%d'
        DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
        if isinstance(v, datetime.datetime):
            return v.strftime(DATETIME_FORMAT)
        if isinstance(v, datetime.date):
            return v.strftime(DATE_FORMAT)
        return v

    def format_date_rows(self, rows):
        for row in rows:
            yield dict((k, self.format_date(v)) for k, v in row.items())

    def handle_resource(self, resource, spec, parameters, datapackage):        
        return super(DumpToElasticSearch, self)\
                .handle_resource(self.format_date_rows(resource), 
                                 spec, parameters, datapackage)


DumpToElasticSearch()()