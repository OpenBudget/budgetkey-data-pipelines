from datapackage_pipelines_elasticsearch.processors.dump.to_index import ESDumper
from tableschema_elasticsearch.mappers import MappingGenerator


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
                    "type":     "text",
                    "analyzer": "hebrew"
                }
          }
        return prop


class DumpToElasticSearch(ESDumper):
    
    def __init__(self):
        super(DumpToElasticSearch, self).__init__(
            mapper_cls=BoostingMappingGenerator,
            index_settings={
                "index.mapping.coerce": True
            }
        )


DumpToElasticSearch()()