import dataflows as DF
from datetime import date


def filter_tamat():
    def func(rows):
        for row in rows:
            moe = False
            mol = False
            for record in row['official']:
                if record.get('source') == 'moe' and not moe:
                    moe = True
                elif record.get('source') == 'mol' and not mol:
                    mol = True
            if moe and mol:
                yield row
        return rows
    return func

def convert_to_spec():
    today = date.today().strftime('%d/%m/%Y')

    def func(row):
        official = row.get('official', [])

        moe_records = sorted(
            [r for r in official if r.get('source') == 'moe'],
            key=lambda r: r.get('school_year') or '', reverse=True
        )
        mol_records = [r for r in official if r.get('source') == 'mol']

        moe = moe_records[0] if moe_records else {}
        mol = mol_records[0] if mol_records else {}

        row['uid'] = row.get('_id')
        # חינוך (moe) is the preferred source
        row['symbol_moe'] = moe.get('symbol')
        row['license_status'] = moe.get('license_status')
        row['sector'] = moe.get('sector')
        # עבודה (mol) is the preferred source, fall back to moe
        row['name'] = mol.get('name') or moe.get('name')
        row['symbol_mol'] = mol.get('symbol')
        row['owner_legal'] = mol.get('org_name') or moe.get('owner')
        row['city'] = row.get('city')
        row['address'] = row.get('formatted_address')
        row['address_city'] = row.get('city')
        row['phone_main'] = mol.get('phone') or moe.get('phone')
        row['manager_name'] = mol.get('manager_name') or moe.get('manager_name')
        row['facility_type'] = mol.get('facility_type')
        row['daycare_type'] = mol.get('daycare_type')
        row['admission_committee'] = mol.get('admission_committee')
        row['total_places_babies'] = mol.get('total_places_babies')
        row['total_places_toddlers'] = mol.get('total_places_toddlers')
        row['total_places_adults'] = mol.get('total_places_adults')
        row['available_places_babies'] = mol.get('available_places_babies')
        row['available_places_toddlers'] = mol.get('available_places_toddlers')
        row['available_places_adults'] = mol.get('available_places_adults')
        row['last_updated'] = today
        row['school_year'] = moe.get('school_year')
        # הסדנא לידע ציבורי (computed)
        row['no_address'] = not bool(row.get('formatted_address'))
        row['lat_y'] = mol.get('coord_y') or moe.get('coord_y')
        row['lon_x'] = mol.get('coord_x') or moe.get('coord_x')
        row['lat_WGS84'] = row.get('lat')
        row['long_WGS84'] = row.get('lng')

    return DF.Flow(
        DF.add_field('uid', 'string'),
        DF.add_field('name', 'string'),
        DF.add_field('symbol_moe', 'string'),
        DF.add_field('symbol_mol', 'string'),
        DF.add_field('owner_legal', 'string'),
        DF.add_field('address', 'string'),
        DF.add_field('address_city', 'string'),
        DF.add_field('no_address', 'boolean'),
        DF.add_field('lat_y', 'number'),
        DF.add_field('lon_x', 'number'),
        DF.add_field('lat_WGS84', 'number'),
        DF.add_field('long_WGS84', 'number'),
        DF.add_field('phone_main', 'string'),
        DF.add_field('manager_name', 'string'),
        DF.add_field('license_status', 'string'),
        DF.add_field('sector', 'string'),
        DF.add_field('daycare_type', 'string'),
        DF.add_field('facility_type', 'string'),
        DF.add_field('admission_committee', 'boolean'),
        DF.add_field('total_places_babies', 'integer'),
        DF.add_field('total_places_toddlers', 'integer'),
        DF.add_field('total_places_adults', 'integer'),
        DF.add_field('available_places_babies', 'integer'),
        DF.add_field('available_places_toddlers', 'integer'),
        DF.add_field('available_places_adults', 'integer'),
        DF.add_field('last_updated', 'string'),
        DF.add_field('school_year', 'string'),
        func,
        DF.delete_fields(['_id', 'formatted_address', 'lat', 'lng', 'official', 'facility_kind', 'city']),
    )

def scrape(prefix='/var/datapackages'):

    return DF.Flow(
        DF.load(f'{prefix}/facilities/all/datapackage.json'),
        filter_tamat(),
        convert_to_spec(),
        DF.update_resource(-1, name='tamat-facilities', path='tamat-facilities.csv'),
        DF.printer()
    )

def flow(*_):
    return DF.Flow(
        scrape(),
        DF.dump_to_path('/var/datapackages/facilities/tamat'),
        DF.dump_to_sql(dict(
            facilities_tamat={'resource-name': 'tamat-facilities'}
        )),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )
    

if __name__ == '__main__':
    DF.Flow(
        scrape(prefix='https://next.obudget.org/datapackages')
    ).process()