import os
import json
from sqlalchemy import create_engine

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines_budgetkey.common.format_number import format_number

engine = create_engine(os.environ['DPP_DB_ENGINE'])


def get_single_result(query):
    results = engine.execute(query)
    results = [dict(r)['x'] for r in results][0]
    return results


def get_total_cities():
    q = """
WITH a AS
  (SELECT distinct(jsonb_array_elements(association_activity_region_list))
FROM guidestar_processed
WHERE association_status_active)
  SELECT count(1) AS x
  FROM a
"""
    return get_single_result(q)


def get_total_orgs():
    q = """
SELECT count(1) AS x
FROM guidestar_processed
WHERE association_status_active
"""
    return get_single_result(q)


def get_total_foas():
    q = """
SELECT count(DISTINCT association_field_of_activity) AS x
FROM guidestar_processed
WHERE association_status_active
"""    
    return get_single_result(q)


def get_total_received():
    q = """
SELECT sum(received_amount) AS x
FROM entities_processed
JOIN guidestar_processed USING (id)
"""
    return get_single_result(q)


def get_district_totals():
    foas_q = """
with a as (SELECT association_field_of_activity as foa, jsonb_array_elements(association_activity_region_districts) as district, count(1) as cnt
FROM guidestar_processed
GROUP BY 1, 2),
b as (select district, sum(cnt) as district_total from a group by 1),
c as (select foa, district, cnt/district_total as foa_district_pct from a join b using (district)),
d as (select foa, sum(foa_district_pct)/7 as foa_pct_avg from c group by 1),
e as (select foa, district, (foa_district_pct - foa_pct_avg) * log(cnt) as score from a join c using(foa, district) join d using (foa))
SELECT district,
       foa,
       rank
FROM
  (SELECT e.*,
          rank() OVER (PARTITION BY district
                       ORDER BY score DESC) AS rank
   FROM e) r
where rank<=3
order by district, rank"""
    results = engine.execute(foas_q)
    results = [dict(r) for r in results]
    ret = {}
    for r in results:
        ret.setdefault(r['district'], dict(totals=0, foas=[]))['foas'].append(r['foa'])
    
    totals_q="""
SELECT jsonb_array_elements(association_activity_region_districts) as district,
       count(1) as count
FROM guidestar_processed
GROUP BY 1"""
    results = engine.execute(totals_q)
    results = [dict(r) for r in results]
    for r in results:
        ret[r['district']]['totals'] = r['count']
    return ret


def get_total_payer_amounts():
    q = """
    select payer, sum(amount) as amount
    from united_spending
    join guidestar_processed on (id=entity_id)
    where amount > 0 
    group by 1
    order by 2 desc
    """
    # removed:
    # and association_status_active

    results = engine.execute(q)
    results = [dict(r) for r in results]
    return results    


def add_main_page_report(res):
    yield from res
    row = dict(
        doc_id='reports/ngos-main-page',
        key='ngos-main-page',
        details=dict(
            total_active_cities=get_total_cities(),
            total_active_orgs=get_total_orgs(),
            total_active_foas=get_total_foas(),
            total_received=get_total_received(),
            district_totals=get_district_totals(),
            foa_stats=json.load(open('ngo-foa-circle-pack.json'))
        ),
        charts=[
            {
                'type': 'adamkey',
                'chart': {
                    'values': [
                        dict(
                            label=x['payer'],
                            amount=x['amount'],
                            amount_fmt=format_number(x['amount']),
                        )
                        for x in get_total_payer_amounts()
                    ]
                }                
            }
        ]
    )
    yield row

def process_resources(res_iter):
    first = next(res_iter)
    yield add_main_page_report(first)
    yield from res_iter


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {
            'name': 'charts',
            'type': 'array',
            'es:itemType': 'object',
            'es:index': False
        },
        {
            'name': 'others',
            'type': 'array',
            'es:index': False,
            'es:itemType': 'string',
        }
    ])
    return dp


if __name__ == '__main__':
    params, dp, res_iter = ingest()
    spew(dp, process_resources(res_iter))
