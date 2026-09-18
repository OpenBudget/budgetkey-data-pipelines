# Ministry of Health - Tipat Halav (family health) stations locator
# https://tipatchalavlocater.health.gov.il/TipotChalav
import re
import logging

import requests
import dataflows as DF

URL = 'https://tipatchalavlocater.health.gov.il/api/TipotChalav/GetTipotChalavStationsResult'
MIN_EXPECTED_STATIONS = 500

DAYS = [
    ('sunday', 'א'),
    ('monday', 'ב'),
    ('tuesday', 'ג'),
    ('wednesday', 'ד'),
    ('thursday', 'ה'),
    ('friday', 'ו'),
    ('shabbat', 'ש'),
]
# The site hides the weekly hours of stations with this showHours value (they work by appointment, see remarks)
HOURS_HIDDEN = 2
EMAIL_RE = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]+')


def get_data():
    # The only things the API needs are a JSON content type and a JSON body;
    # an empty filter returns all stations in one response (paging parameters are ignored).
    response = requests.post(URL, json={}, timeout=60)
    response.raise_for_status()
    try:
        stations = response.json()
    except ValueError:
        # Blocked requests get an HTML page with a 200 status
        raise ValueError(f'Expected JSON, got: {response.text[:200]!r}')
    assert isinstance(stations, list) and len(stations) >= MIN_EXPECTED_STATIONS, \
        f'Expected at least {MIN_EXPECTED_STATIONS} stations, got: {str(stations)[:200]}'
    return stations


def clean(value):
    return (value or '').strip() or None


def unique(values):
    return list(dict.fromkeys(v for v in values if v))


def parse_time(value):
    # Tolerates typos in the source such as '08:0', '19-00' and '12:000'
    parts = re.findall(r'\d+', value)
    if len(parts) != 2:
        return None
    hours, minutes = parts[0].zfill(2), parts[1][:2].ljust(2, '0')
    if len(hours) != 2 or int(hours) > 24 or int(minutes) > 59:
        return None
    return f'{hours}:{minutes}'


def day_hours(value, station):
    # '08:30~13:30|16:00~18:00' -> '08:30-13:30,16:00-18:00'
    ranges = []
    for time_range in filter(None, (clean(r) for r in (value or '').split('|'))):
        times = [parse_time(t) for t in time_range.split('~')]
        if len(times) == 2 and all(times):
            ranges.append('-'.join(times))
        else:
            logging.warning('Station %s: unparsable opening hours %r', station, time_range)
            ranges.append(time_range)
    return ','.join(ranges)


def opening_hours(row):
    # OpenStreetMap-style text with Hebrew day names, e.g. 'א-ג 08:00-14:00; ד 08:00-12:00,16:00-18:00'
    if row['showHours'] == HOURS_HIDDEN:
        return None
    groups = []
    for field, day in DAYS:
        hours = day_hours(row[field], row['code'])
        if groups and groups[-1]['hours'] == hours:
            groups[-1]['last'] = day
        else:
            groups.append(dict(first=day, last=day, hours=hours))
    return '; '.join(
        (g['first'] if g['first'] == g['last'] else f'{g["first"]}-{g["last"]}') + ' ' + g['hours']
        for g in groups if g['hours']
    ) or None


def address(row):
    street = ' '.join(filter(None, (clean(row['streetName']), clean(row['buildingNum']))))
    return ', '.join(filter(None, (street, clean(row['cityName'])))) or None


def phone_numbers(row):
    phones = (clean(row.get(f'phone{i}')) for i in range(1, 7))
    # Drop leftovers such as '02' or '99-'
    return unique(p for p in phones if p and len(re.sub(r'\D', '', p)) >= 4)


def emails(row):
    return unique(m for f in ('email', 'email2') for m in EMAIL_RE.findall(row.get(f) or ''))


FIELDS = dict(
    id='string', name='string', folder_num='string',
    status='string', status_code='integer', owner='string', owner_code='integer',
    address='string', address_comments='string', city='string', city_code='string',
    district='string', region='string', lat='number', lng='number',
    phone_numbers='array', fax='string', emails='array', opening_hours='string', notes='string',
)


def convert(row):
    return dict(
        id=clean(row['code']),
        name=clean(row['stationName']),
        folder_num=clean(row['folderNum']),
        status=clean(row['status']),
        status_code=row['statusCode'],
        owner=clean(row['ownerShip']),
        owner_code=row['ownerShipCode'],
        address=address(row),
        address_comments=clean(row['addressComments']),
        city=clean(row['cityName']),
        city_code=str(row['cityCode']) if row['cityCode'] else None,
        district=clean(row['district']),
        region=clean(row['region']),
        lat=row['yCordinate'] or None,
        lng=row['xCordinate'] or None,
        phone_numbers=phone_numbers(row),
        fax=clean(row['fax']),
        emails=emails(row),
        opening_hours=opening_hours(row),
        notes=clean(row['remarks']),
    )


def scrape():
    return DF.Flow(
        (convert(row) for row in get_data() if clean(row['code'])),
        *[
            DF.set_type(name, type=type_, **({'es:itemType': 'string'} if type_ == 'array' else {}))
            for name, type_ in FIELDS.items()
        ],
        DF.set_primary_key(['id']),
        DF.update_resource(-1, name='tipat-halav', path='tipat-halav.csv'),
    )


def flow(*_):
    return DF.Flow(
        scrape(),
        DF.dump_to_path('/var/datapackages/facilities/tipat-halav'),
        DF.dump_to_sql(dict(
            facilities_tipat_halav={'resource-name': 'tipat-halav'}
        )),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )


if __name__ == '__main__':
    DF.Flow(
        scrape(),
        DF.printer()
    ).process()
