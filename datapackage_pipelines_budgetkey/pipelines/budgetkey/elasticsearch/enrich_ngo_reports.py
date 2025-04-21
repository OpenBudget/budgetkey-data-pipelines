import os

from datapackage_pipelines.wrapper import ingest, spew

from sqlalchemy import create_engine

parameters, dp, res_iter = ingest()

engine = create_engine(os.environ['DPP_DB_ENGINE'])


DISTRICT_INFO = '''
SELECT district_2015 AS district,
       sum(index_socioeconomic_2013_cluster_from_1_to_10_1_lowest_most*population_end_2013_number_all_1000s)/sum(population_end_2013_number_all_1000s) AS socioeconomic,
       sum(population_end_2013_number_all_1000s*1000) AS population,
       sum(uses_land_commerce_offices_area_km2_2013 + uses_land_culture_leisure_recreation_sport_area_km2_2013 + uses_land_education_education_area_km2_2013 + uses_land_gardening_decoration_park_public_area_km2_2013 + uses_land_health_welfare_area_km2_2013 + uses_land_industry_area_km2_2013 + uses_land_infrastructure_transporation_area_km2_2013 + uses_land_residential_area_km2_2013 + uses_land_services_public_area_km2_2013) AS built_area
FROM lamas_muni
GROUP BY district_2015
'''

ACTIVITY_FIELD_INFO = '''
SELECT association_field_of_activity,
       sum(received_amount) as total_received_in_field
FROM entities_processed
JOIN guidestar_processed USING (id)
GROUP BY 1
'''

ACTIVITY_FIELD_AVG = '''
WITH a AS
  (SELECT association_field_of_activity,
          sum(received_amount) AS total_for_field
   FROM entities_processed
   JOIN guidestar_processed USING (id)
   GROUP BY 1)
SELECT avg(total_for_field) AS avg_field_received
FROM a'''


def get_district_info():
    query = DISTRICT_INFO
    result = engine.execute(query)
    result = list(dict(r) for r in result)
    result = dict((x.pop('district'), x) for x in result)
    return result


def get_field_of_activity_info():
    query = ACTIVITY_FIELD_AVG
    result = engine.execute(query)
    avg_field_received = dict(next(iter(result)))['avg_field_received']
    query = ACTIVITY_FIELD_INFO
    result = engine.execute(query)
    result = list(dict(r) for r in result)
    result = dict((x.pop('association_field_of_activity'), x) for x in result)
    for x in result.values():
        x['avg_field_received'] = avg_field_received
    return result
    

def process_resource(res_):
    di = get_district_info()
    foai = get_field_of_activity_info()
    missing_foas = []
    for row in res_:
        if row['key'].startswith('ngo-district-report'):
            district = row['details']['district']
            if district not in di:
                continue
            details = row['details']
            for k, v in di[district].items():
                details[k] = float(v)
            row['details'] = details
        elif row['key'].startswith('ngo-activity-report'):
            foa = row['details']['field_of_activity']
            details = row['details']
            foa_deets = foai.get(foa)
            if foa_deets is None:
                foa_deets = {}
                missing_foas.append(foa)
            for k, v in foa_deets.items():
                details[k] = float(v) if v is not None else None
            row['details'] = details
        yield row
    assert len(missing_foas) == 0, 'Missing foas:\n%s' % '\n'.join(missing_foas)


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_

spew(dp, process_resources(res_iter))
