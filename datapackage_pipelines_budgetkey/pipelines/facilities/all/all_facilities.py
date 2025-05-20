import requests
import re
import os
import hashlib
import json

from pyproj import Transformer

import dataflows as DF
from kvfile.kvfile_sqlite import KVFileSQLite as KVFile

# import dotenv
# dotenv.load_dotenv()

GOOGLE_MAPS_API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY')
GOVMAP_API_KEY = os.environ.get('GOVMAP_API_KEY')
GOVMAP_REQUEST_ORIGIN = os.environ.get('GOVMAP_REQUEST_ORIGIN')
GOVMAP_GEOCODE_API = os.environ.get('GOVMAP_GEOCODE_API')
GOVMAP_AUTH_API = os.environ.get('GOVMAP_AUTH_API')
DIGIT = re.compile(r'\d')

os.makedirs('/var/datapackages/facilities/all/', exist_ok=True)

transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)

def hasher(value: str):
    return hashlib.md5(value.encode()).hexdigest()[:12]

def to_wgs84():
    def func(row):
        x = row['record'].get('coord_x')
        y = row['record'].get('coord_y')
        if x and y:
            x = int(x)
            y = int(y)
            lng, lat = transformer.transform(x, y)
            row['lat'] = lat
            row['lng'] = lng
    return DF.Flow(
        DF.add_field('lat', 'number'),
        DF.add_field('lng', 'number'),
        func
    )

def govmap_session():
    try:
        token = GOVMAP_API_KEY
        auth_data = dict(
            api_token=token, user_token='', domain=GOVMAP_REQUEST_ORIGIN, token=''
        )
        headers = dict(
            auth_data=json.dumps(auth_data),
            Origin=GOVMAP_REQUEST_ORIGIN,
            Referer=GOVMAP_REQUEST_ORIGIN,
        )

        resp = requests.post(GOVMAP_AUTH_API,
                        json=dict(),
                        headers=headers)
        headers = dict(
            auth_data=json.dumps(resp.json()),
            Origin=GOVMAP_REQUEST_ORIGIN,
            Referer=GOVMAP_REQUEST_ORIGIN,
        )
    except Exception as e:
        raise

    session = requests.Session()
    session.headers.update(headers)
    return session    

def govmap_geocode():
    cache = KVFile(location='/var/datapackages/facilities/all/govmap-cache.sqlite')
    session = govmap_session()
    def func(row):
        address = row['record'].get('address')
        if address and not row['record'].get('coord_x') and not row['record'].get('coord_y'):
            address_hash = hasher(address)
            hit = False
            cached = cache.get(address_hash, default=None)
            update = None
            if cached is not None:
                hit = True
                update = cached
            else:
                geocode_req = dict(
                    keyword=address, type=0,
                )
                resp = session.post(GOVMAP_GEOCODE_API, json=geocode_req)
                if resp.status_code not in (200, 404):
                    assert False
                try:
                    resp = resp.json()
                except json.decoder.JSONDecodeError:
                    resp = dict(status=None, errorCode=None)

                if resp['status'] == 0 and resp['errorCode'] == 0:
                    assert 'data' in resp and len(resp['data']) > 0, str(resp)
                    resp = resp['data'][0]
                    print(resp)
                    assert resp['ResultType'] in (1, ), str(resp)
                    accuracy = resp['DescLayerID'].replace('NEW', '').strip('_')
                    if accuracy in ('POI_MID_POINT', 'ADDR_V1', 'ADDRESS_POINT', 'ADDRESS'):
                        row['formatted_address'] = resp['ResultLable']
                        update = dict(
                            coord_x=resp['X'],
                            coord_y=resp['Y'],
                            formatted_address=resp['ResultLable'],
                        )
                        cache.set(address_hash, update)
                    else:
                        cache.set(address_hash, {})
                else:
                    if resp['message'] == 'כתובת לא נמצאה':
                        cache.set(address_hash, {})
                    else:
                        print('ERROR', address, resp)
            if update:
                row['formatted_address'] = update['formatted_address']
                row['city'] = re.split('[,|]', row['formatted_address'])[-1].strip()
                row['record']['coord_x'] = update['coord_x']
                row['record']['coord_y'] = update['coord_y']
            if not hit:
                print(f"GOVMAP Address {address} -> {update}")

    return DF.Flow(
        func
    )

def gmaps_geocode():
    cache = KVFile(location='/var/datapackages/facilities/all/google-geocode-cache.sqlite')
    def func(row):
        address = row['record'].get('address')
        lat = row['lat']
        lng = row['lng']
        city = row['city']
        formatted_address = row['formatted_address']
        if address and not lat and not lng and not city and not formatted_address:
            address_hash = hasher(address)
            update = {}
            hit = False
            cached = cache.get(address_hash, default=None)
            if cached is not None:
                hit = True
                update = cached
            else:
                url = f'https://maps.googleapis.com/maps/api/geocode/json'
                params = {
                    'address': address,
                    'key': GOOGLE_MAPS_API_KEY,
                    'language': 'iw',
                    'components': 'country:IL',
                }
                response = requests.get(url, params=params)
                result = response.json()
                if result['status'] == 'OK':
                    result = result['results'][0]
                    accuracy = result['geometry']['location_type']
                    if accuracy in {'ROOFTOP', 'RANGE_INTERPOLATED'}:
                        location = result['geometry']['location']
                        update.update(dict(
                            lat=location['lat'],
                            lng=location['lng'],
                            formatted_address=result['formatted_address'],
                        ))
                    for component in result['address_components']:
                        if 'locality' in component['types']:
                            update.update(dict(
                                city=component['long_name'],
                            ))
                    cache.set(address_hash, update)
                else:
                    if result['status'] == 'ZERO_RESULTS':
                        cache.set(address_hash, {})
                    else:
                        print('ERROR', address, result)
            if not hit:
                print(f"Address {address} -> {update}")
            row.update(update)
    return DF.Flow(
        func
    )

def add_to_obj(prefixes):
    def func(row):
        row['record'] = {}
        for k, v in row.items():
            for prefix in prefixes:
                if k.startswith(prefix):
                    _k = k[len(prefix) + 1:]
                    row['record'][_k] = v
                    break
        row['record']['source'] = row.get('source')
    return DF.Flow(
        DF.add_field('record', 'object'),
        func
    )

def fix_phone_numbers():
    def func(row):
        number = row['record'].get('phone')
        if number:
            number = number.strip()
            digits = ''.join(DIGIT.findall(number))
            if len(digits) > 10 and digits.startswith('972'):
                digits = digits[3:]
                if len(digits) < 10 and not digits.startswith('0'):
                    digits = '0' + digits
            if len(digits) == 9 and digits.startswith('0'):
                digits = [digits[0:2], digits[2:5], digits[5:]]
            elif len(digits) == 10 and digits.startswith('0'):
                digits = [digits[0:3], digits[3:6], digits[6:]]
            elif len(digits) == 10 and digits.startswith('1'):
                digits = [digits[:1], digits[1:4], digits[4:]]
            else:
                digits = None
            if digits:
                number = '-'.join(digits)
            if number.startswith('00'):
                number = None
            if number:
                # if number != row['record']['phone']:
                #     print(f"Fixing phone number: {row['record']['phone']} -> {number}")
                row['record']['phone'] = number
    return func

def concatenate_lists():
    return DF.Flow(
        add_to_obj(['moe', 'lab', 'welfare']),
        DF.select_fields(['_id', 'record']),
        DF.concatenate(dict(
            _id=[],
            record=[],
        ), dict(name='records')),
        fix_phone_numbers(),
        DF.add_field('formatted_address', 'string'),
        DF.add_field('city', 'string'),
        govmap_geocode(),
        to_wgs84(),
        gmaps_geocode(),
        DF.printer(),
    )

def intersects(k1, k2):
    people_intersects = len(k1['people'].intersection(k2['people'])) > 0
    places_intersects = len(k1['places'].intersection(k2['places'])) > 0
    phones_intersects = len(k1['phones'].intersection(k2['phones'])) > 0
    names_intersects = len(k1['names'].intersection(k2['names'])) > 0
    ids_intersects = len(k1['ids'].intersection(k2['ids'])) > 0
    return ids_intersects or (places_intersects and (people_intersects or phones_intersects or names_intersects))

def dedupe():
    def func(rows):
        keys = []
        row_mapping = {}
        for row in rows:
            row_mapping[row['_id']] = row
            key = dict(
                names=set(),
                places=set(),
                phones=set(),
                people=set(),
                ids=set(),
            )
            record = row['record']
            for k, v in record.items():
                if isinstance(v, str):
                    v = v.strip()                    
                    if v and len(v) > 2:
                        if k == 'manager_name':
                            key['people'].add(' '.join(sorted(v.split())))
                        if k == 'address':
                            key['places'].add(v)
                        if k == 'phone':
                            key['phones'].add(v)
                        if k == 'name':
                            key['names'].add(v)
                        if k == 'mol_symbol':
                            key['ids'].add(v)
            if row['_id']:
                key['ids'].add(row['_id'])
            if row.get('formatted_address'):
                key['places'].add(row.get('formatted_address'))
            for i, key_ in enumerate(keys):
                if intersects(key, key_):
                    if i < 10:
                        print(f'{i}: INTERSECTS:\n  {key}\n  {key_}')
                    key_['ids'].update(key['ids'])
                    key_['names'].update(key['names'])
                    key_['places'].update(key['places'])
                    key_['phones'].update(key['phones'])
                    key_['people'].update(key['people'])
                    break
            else:
                keys.append(key)

        for key in keys:
            # print(key)
            ids = sorted(key['ids'])
            first_row = row_mapping[ids[0]]
            rec = dict(
                _id='-'.join(ids),
                lat=first_row['lat'],
                lng=first_row['lng'],
                formatted_address=first_row['formatted_address'],
                city=first_row['city'],
                records=[],
            )
            for id in ids:
                if id in row_mapping:
                    rec['records'].append(row_mapping[id]['record'])
                else:
                    print(f"Couldn’t find record for ID: {id}")
            assert len(rec['records']) > 0, str(rec)
            yield rec
    return DF.Flow(
        DF.add_field('records', 'array'),
        func,
        DF.delete_fields(['record']),
    )
    
def fix_mol_symbol():
    def func(rows):
        for row in rows:
            if row['record'].get('mol_symbol'):
                row['record']['mol_symbol'] = f'mol-{row["record"]["mol_symbol"]}'
        return rows
    return DF.Flow(
        func,
        DF.add_field('records', 'array'),
    )

def extract_city():
    def func(row):
        if not row['city']:
            
            for record in row['records']:
                if record.get('city'):
                    row['city'] = record['city']
                    break
    return func

def scrape(prefix='/var/datapackages'):
    flows = [
        DF.load(f'{prefix}/facilities/labor/datapackage.json'),
        DF.load(f'{prefix}/facilities/education/datapackage.json'),
        DF.load(f'{prefix}/facilities/welfare/datapackage.json'),
    ]

    return DF.Flow(
        *flows,
        concatenate_lists(),
        dedupe(),
        extract_city(),
        DF.update_resource(-1, name='all-facilities', path='all-facilities.csv'),
        DF.printer()
    )

def flow(*_):
    return DF.Flow(
        scrape(),
        DF.dump_to_path('/var/datapackages/facilities/all'),
        DF.dump_to_sql(dict(
            facilities_all={'resource-name': 'all-facilities'}
        )),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )
    

if __name__ == '__main__':
    DF.Flow(
        scrape(prefix='https://next.obudget.org/datapackages')
    ).process()