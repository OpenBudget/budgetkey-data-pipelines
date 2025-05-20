import requests
import datetime
import dataflows as DF

def get_data():
    url = 'https://daycareclasssearch.labor.gov.il/api/FramesSearch/GetFramesList'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Content-Type': 'application/json',
        'Connection': 'keep-alive',
        'Referer': 'https://daycareclasssearch.labor.gov.il/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Priority': 'u=0'
    }

    data = []
    current_year = datetime.datetime.now().year
    for frame_type in range(1, 4):
        post_data = {"Year":current_year,"FrameType":frame_type,"FrameId":""}
        response = requests.post(url, headers=headers, json=post_data, timeout=30)
        if response.status_code == 200:
            frames_list = response.json()['Frames']
            data.extend(frames_list)
        else:
            print("Request failed with status code:", response.status_code)
    return data

def construct_address(record):
    address_parts = [
        record.get('Address', ''),
        record.get('TownName', ''),
    ]
    return ', '.join(filter(None, address_parts))

def scrape():
    return DF.Flow(
        get_data(),
        DF.checkpoint('mol'),
        DF.add_field('lab_symbol', 'string', lambda r: str(r['FrameId'])),
        DF.add_field('lab_name', 'string', lambda r: r['FrameName']),
        DF.add_field('lab_city', 'string', lambda r: r['TownName']),
        DF.add_field('lab_address', 'string', construct_address),
        DF.add_field('lab_manager_name', 'string', lambda r: r['ManagerName']),
        DF.add_field('lab_phone', 'string', lambda r: r['Phone']),
        DF.add_field('lab_org_name', 'string', lambda r: r['OrganizationName']),
        DF.add_field('lab_facility_type', 'string', lambda r: r['FrameTypeDesc']),
        DF.add_field('lab_daycare_type', 'string', lambda r: r['DaycareTypeDesc']),
        DF.add_field('lab_total_places_babies', 'integer', lambda r: r['NumOfApprovedBabiesPlaces']),
        DF.add_field('lab_available_places_babies', 'integer', lambda r: r['NumOfAvailableBabiesPlaces']),
        DF.add_field('lab_total_places_toddlers', 'integer', lambda r: r['NumOfApprovedToddlersPlaces']),
        DF.add_field('lab_available_places_toddlers', 'integer', lambda r: r['NumOfAvailableToddlersPlaces']),
        DF.add_field('lab_total_places_adults', 'integer', lambda r: r['NumOfApprovedAdultsPlaces']),
        DF.add_field('lab_available_places_adults', 'integer', lambda r: r['NumOfAvailableAdultsPlaces']),
        DF.add_field('lab_admission_committee', 'boolean', lambda r: bool(r['IsThereAdmissionsCommittee'])),
        DF.filter_rows(lambda r: bool(r['lab_symbol'])),
        DF.add_field('_id', 'string', lambda r: f'mol-{r["lab_symbol"]}'),
        DF.select_fields(['_id', 'lab_.+']),
        DF.add_field('source', 'string', 'mol'),
        DF.update_resource(-1, name='mol', path='mol.csv'),
        DF.printer()
    )


def flow(*_):
    return DF.Flow(
        scrape(),
        DF.dump_to_path('/var/datapackages/facilities/labor'),
        DF.dump_to_sql(dict(
            facilities_labor={'resource-name': 'mol'}
        )),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )

if __name__ == '__main__':
    scrape().process()