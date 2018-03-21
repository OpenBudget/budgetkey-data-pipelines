import os

from datapackage_pipelines.wrapper import ingest, spew

from sqlalchemy import create_engine

parameters, dp, res_iter = ingest()

engine = create_engine(os.environ['DPP_DB_ENGINE'])


DISTRICT_INFO = '''
SELECT district_2015 AS district,
       sum(index_socioeconomic_2013_cluster_from_1_to_10_1_lowest_most*population_end_2013_number_all_1000s)/sum(population_end_2013_number_all_1000s) AS socioeconomic,
       sum(population_end_2013_number_all_1000s*100) AS population,
       sum(uses_land_commerce_offices_area_km2_2013 + uses_land_culture_leisure_recreation_sport_area_km2_2013 + uses_land_education_education_area_km2_2013 + uses_land_gardening_decoration_park_public_area_km2_2013 + uses_land_health_welfare_area_km2_2013 + uses_land_industry_area_km2_2013 + uses_land_infrastructure_transporation_area_km2_2013 + uses_land_residential_area_km2_2013 + uses_land_services_public_area_km2_2013) AS built_area
FROM lamas_muni
GROUP BY district_2015
'''


def get_district_info():
    query = DISTRICT_INFO
    result = engine.execute(query)
    result = list(dict(r) for r in result)
    result = dict((x.pop('district'), x) for x in result)
    return result

def process_resource(res_):
    di = get_district_info()
    for row in res_:
        if row['key'].startswith('ngo-district-report'):
            district = row['details']['district']
            details = row['details']
            for k, v in di[district].items():
                details[k] = float(v)
            row['details'] = details
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_

spew(dp, process_resources(res_iter))
