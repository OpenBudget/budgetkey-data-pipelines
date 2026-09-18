import requests
import re
import os
import hashlib

from pyproj import Transformer

import dataflows as DF
from kvfile.kvfile_sqlite import KVFileSQLite as KVFile

# import dotenv
# dotenv.load_dotenv()

GOOGLE_MAPS_API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY')
GOVMAP_API_KEY = os.environ.get('GOVMAP_API_KEY')
GOVMAP_REQUEST_ORIGIN = os.environ.get('GOVMAP_REQUEST_ORIGIN')
GOVMAP_SEARCH_API = 'https://www.govmap.gov.il/api/search-service/api-search'
GOVMAP_POINT_TYPES = ('address', 'poi')
GOVMAP_POINT = re.compile(r'POINT \(([\d.]+) ([\d.]+)\)')
DIGIT = re.compile(r'\d')
DIGITS = re.compile(r'\d+')
# A match in the wrong city is worse than a match for the wrong house in the right street
SUSPICION_WEIGHTS = dict(city=2, house_number=1)

os.makedirs('/var/datapackages/facilities/all/', exist_ok=True)

transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)

def hasher(value: str):
    return hashlib.md5(value.encode()).hexdigest()[:12]

def normalize(text):
    # Ignore punctuation, spacing and spelling variants (e.g. 'מודיעין עלית' / 'מודיעין-עילית', 'בני ברק' / 'בניברק')
    text = re.sub(r'[^0-9a-zא-ת]', '', (text or '').lower())
    return re.sub('[יו]', '', text)

def suspicious(record, *texts):
    # Reasons to doubt that a geocoder's match (as described by `texts`) is really the record's address
    reasons = []
    texts = [t for t in texts if t]
    city = normalize(record.get('city'))
    if city and not any(city in normalize(t) for t in texts):
        reasons.append('city')
    house_number = DIGITS.search((record.get('address') or '').split(',')[0])
    if house_number and int(house_number.group()) > 0:
        if not any(house_number.group() in DIGITS.findall(t) for t in texts):
            reasons.append('house_number')
    return reasons

def suspicion_score(reasons):
    return sum(SUSPICION_WEIGHTS[r] for r in reasons)

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
    session = requests.Session()
    session.headers.update({
        # Requests without a browser-like user agent are blocked (403)
        'User-Agent': 'Mozilla/5.0',
        # The API key is validated against the request's origin
        'Origin': GOVMAP_REQUEST_ORIGIN,
    })
    return session

def govmap_geocode():
    cache = KVFile(location='/var/datapackages/facilities/all/govmap-cache.sqlite')
    session = govmap_session()
    def func(row):
        record = row['record']
        address = record.get('address')
        if record.get('coord_x') and record.get('coord_y'):
            row['geocode_source'] = 'source'
        elif address and not record.get('coord_x') and not record.get('coord_y'):
            address_hash = hasher(address)
            hit = False
            cached = cache.get(address_hash, default=None)
            update = None
            if cached is not None:
                hit = True
                update = cached
            else:
                search_req = dict(
                    apiKey=GOVMAP_API_KEY, searchText=address,
                )
                resp = session.post(GOVMAP_SEARCH_API, json=search_req)
                assert resp.status_code == 200, f'{resp.status_code}: {resp.text[:200]}'
                results = resp.json().get('results')
                update = {}
                if results:
                    # Only the top result counts, and only if it's an actual point (not a street / settlement mid-point)
                    result = results[0]
                    print(result)
                    centroid = GOVMAP_POINT.fullmatch(result.get('centroid') or '')
                    if result['type'] in GOVMAP_POINT_TYPES and centroid:
                        id_parts = result['id'].split('|')
                        update = dict(
                            coord_x=float(centroid.group(1)),
                            coord_y=float(centroid.group(2)),
                            formatted_address=result['text'],
                            # 'text' might abbreviate the city's name ('ת.א', 'בש'), in which case this holds the full names
                            original_text=result.get('originalText'),
                            # address ids may look like 'address|ADDR|<num>|<street>|<house>|<city>'
                            city=id_parts[5] if len(id_parts) == 6 else None,
                        )
                cache.set(address_hash, update)
            if update:
                row['formatted_address'] = update['formatted_address']
                if 'city' in update:
                    row['city'] = update['city']
                else:
                    # Cached results of the old geocoding API
                    row['city'] = re.split('[,|]', row['formatted_address'])[-1].strip()
                # The source's city is preferable, as govmap might abbreviate city names
                row['city'] = record.get('city') or row['city']
                record['coord_x'] = update['coord_x']
                record['coord_y'] = update['coord_y']
                row['geocode_source'] = 'govmap'
                reasons = suspicious(record, update['formatted_address'], update.get('original_text'))
                row['geocode_suspicious'] = ','.join(reasons) or None
            if not hit:
                print(f"GOVMAP Address {address} -> {update}")

    return DF.Flow(
        func
    )

def gmaps_geocode():
    cache = KVFile(location='/var/datapackages/facilities/all/google-geocode-cache.sqlite')
    def func(row):
        record = row['record']
        address = record.get('address')
        lat = row['lat']
        lng = row['lng']
        city = row['city']
        formatted_address = row['formatted_address']
        not_geocoded = not lat and not lng and not city and not formatted_address
        # A suspicious govmap match gets a second opinion
        govmap_reasons = (row['geocode_suspicious'] or '').split(',') if row['geocode_source'] == 'govmap' else []
        govmap_reasons = list(filter(None, govmap_reasons))
        if address and (not_geocoded or govmap_reasons):
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
            reasons = suspicious(record, update.get('formatted_address'), update.get('city')) if 'lat' in update else None
            if govmap_reasons:
                # Google's match replaces govmap's only if it's an actual point and it's less suspicious
                use_google = reasons is not None and suspicion_score(reasons) < suspicion_score(govmap_reasons)
                print(f"SUSPICIOUS {address}: govmap {formatted_address!r} {govmap_reasons}, "
                      f"google {update.get('formatted_address')!r} {reasons} -> {'google' if use_google else 'govmap'}")
                if not use_google:
                    return
                # These are govmap's, google's location is only in lat / lng
                del record['coord_x']
                del record['coord_y']
            row.update(update)
            if reasons is not None:
                row['geocode_source'] = 'google'
                row['geocode_suspicious'] = ','.join(reasons) or None
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
        DF.add_field('geocode_source', 'string'),
        DF.add_field('geocode_suspicious', 'string'),
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
                _id=ids[0],
                lat=first_row['lat'],
                lng=first_row['lng'],
                formatted_address=first_row['formatted_address'],
                city=first_row['city'],
                geocode_source=first_row['geocode_source'],
                geocode_suspicious=first_row['geocode_suspicious'],
                official=[],
            )
            for id in ids:
                if id in row_mapping:
                    rec['official'].append(row_mapping[id]['record'])
                else:
                    print(f"Couldn’t find record for ID: {id}")
            assert len(rec['official']) > 0, str(rec)
            yield rec
    return DF.Flow(
        DF.add_field('official', 'array'),
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
            for record in row['official']:
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
        DF.add_field('facility_kind', 'string', 'education'),
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