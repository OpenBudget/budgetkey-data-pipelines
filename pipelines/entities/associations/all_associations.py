import demjson
import requests

from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()


def get_associations():
    all_db_url = 'http://www.justice.gov.il/DataGov/Corporations/Associations.json.txt'
    all_db = requests.get(all_db_url).content
    all_db = demjson.decode(all_db)
    for rec in all_db['Data']:
        yield dict(
            (k, str(v)) for k, v in rec.items()
        )

resource = {
    'name': 'association-registry',
    'path': 'data/associations.csv',
    'schema': {
        'fields': [
            {'name': 'Association_Number', 'type': 'string'},
            {'name': 'Association_Name', 'type': 'string'},
            {'name': 'Association_Registration_Date', 'type': 'string'},
        ]
    }
}

datapackage['resources'].append(resource)

spew(datapackage, [get_associations()])
