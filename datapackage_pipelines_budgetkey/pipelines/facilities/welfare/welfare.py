# Ministry of Welfare - CKAN Datastore
# Sample access: https://data.gov.il/api/3/action/datastore_search?resource_id=de069ddf-bcbc-4754-bda0-84873a353f7b&limit=5

import requests
import dataflows as DF

def get_data():
    # Fetch data iteratively
    url = 'https://data.gov.il/api/3/action/datastore_search'
    resource_id = 'de069ddf-bcbc-4754-bda0-84873a353f7b'
    limit = 1000
    offset = 0
    data = []
    types = set()
    while True:
        params = {'resource_id': resource_id, 'limit': limit, 'offset': offset}
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            records = response.json()['result']['records']
            if len(records) == 0:
                break
            for record in records:
                if record.get('Type_Descr'):
                    if record['Type_Descr'] not in {
                        'מרכז הורים ילדים',
                        'מעון יום',
                        'מעון רב תכליתי',
                        'מועדונית',
                    }:
                        continue
                if record.get('Head_Department'):
                    if record['Head_Department'].strip() not in {
                        'אוטיסטים',
                        'נוער וצעירים',
                        'ילד ונוער',
                    }:
                        continue
                if record.get('From_Age') and record.get('From_Age') > 6:
                    continue
                if record.get('Telephone'):
                    record['Telephone'] = '0' + str(record['Telephone'])
                yield record
                # data.append(record)
            offset += limit
        else:
            print("Request failed with status code:", response.status_code)
            break
    print(types)

def organization(record):
    kind = record['Owner_Code_Descr']
    org = record.get('Organization')
    if kind:
        kind = kind.strip()
    if kind == 'ארגון מפעיל' and org not in ('0 ללא ארגון', None):
        return org
    if kind in ('ציבורי', 'רשות מקומית', 'פרטי'):
        return kind
    assert False, f"Unknown Owner_Code_Descr: {kind!r}, org: {org!r}"

def s(value):
    if not value:
        return None
    if isinstance(value, str):
        return value.strip()
    return value

def format_address(record):
    address_parts = [
        s(record.get('Adrees', '')),
        s(record.get('City_Name', '')),
    ]
    return ', '.join(filter(None, address_parts))

def scrape():
    return DF.Flow(
        get_data(),
        DF.checkpoint('welfare'),
        DF.update_schema(-1, missingValues=['לא ידוע', 'לא משויך', 'אחר', '0', '']),
        DF.validate(),
        # DF.filter_rows(lambda r: r['Status_des'] == 'פעילה'),
        DF.add_field('welfare_symbol', 'string', lambda r: str(r['Misgeret_Id'])),
        DF.add_field('welfare_name', 'string', lambda r: s(r['Name'])),
        DF.add_field('welfare_facility_type', 'string', lambda r: s(r['Type_Descr'])),
        DF.add_field('welfare_org', 'string', organization),
        DF.add_field('welfare_manager_name', 'string', lambda r: s(r['Maneger_Name'])),
        DF.add_field('welfare_city', 'string', lambda r: s(r['City_Name'])),
        DF.add_field('welfare_address', 'string', format_address),
        DF.add_field('welfare_phone', 'string', lambda r: s(r['Telephone'])),
        DF.add_field('welfare_capacity', 'integer', lambda r: r['Actual_Capacity']),
        DF.add_field('welfare_religion', 'string', lambda r: s(r['Religion'])),
        DF.add_field('welfare_for_gender', 'string', lambda r: s(r['Gender_Descr'])),
        DF.add_field('welfare_from_age', 'integer', lambda r: r['From_Age']),
        DF.add_field('welfare_to_age', 'integer', lambda r: r['To_Age']),
        DF.add_field('welfare_coord_x', 'integer', lambda r: r['GisX']),
        DF.add_field('welfare_coord_y', 'integer', lambda r: r['GisY']),
        DF.filter_rows(lambda r: bool(r['welfare_symbol'])),
        DF.add_field('_id', 'string', lambda r: f'welfare-{r["welfare_symbol"]}'),
        DF.select_fields(['_id', 'welfare_.+']),
        DF.add_field('source', 'string', 'welfare'),
        DF.update_resource(-1, name='welfare', path='welfare.csv'),
        DF.printer()
    )

def flow(*_):
    return DF.Flow(
        scrape(),
        DF.dump_to_path('/var/datapackages/facilities/welfare'),
        DF.dump_to_sql(dict(
            facilities_welfare={'resource-name': 'welfare'}
        )),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )

if __name__ == '__main__':
    scrape().process()