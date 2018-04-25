import os
import json
from sqlalchemy import create_engine

from datapackage_pipelines.wrapper import ingest, spew

engine = create_engine(os.environ['DPP_DB_ENGINE'])


# SPENDING_ANALYSIS_FOR_FOA = """
# SELECT payer, spending
# FROM publisher_foa_analysis
# WHERE field_of_activity='{foa}'
# """

# ALL_DISTRICTS = """
# WITH a AS
#   (SELECT jsonb_array_elements(association_activity_region_districts) AS x,
#           count(1)
#    FROM guidestar_processed
#    GROUP BY 1
#    ORDER BY 2 DESC)
# SELECT x
# FROM a"""

# ALL_FOAS = """
# select distinct(association_field_of_activity) as x from guidestar_processed order by 1
# """


# def get_spending_analysis(foa):
#     query = SPENDING_ANALYSIS_FOR_FOA.format(foa=foa)
#     results = engine.execute(query)
#     results = [dict(r) for r in results]
#     for r in results:
#         r['amount'] = sum(x['amount'] for x in r['spending'])
#     results = sorted(results, key=lambda x: -x['amount'])
#     return results


# def get_distinct_list(query):
#     results = engine.execute(query)
#     results = [dict(r)['x'] for r in results]
#     return results


# all_districts = get_distinct_list(ALL_DISTRICTS)
# all_foas = get_distinct_list(ALL_FOAS)


# def process_row(row, *_):
#     if row['key'].startswith('ngo-activity-report'):
#         details = row['details']
#         foa = details['field_of_activity']
#         foad = details['field_of_activity_display']
#         spending_analysis = get_spending_analysis(foa)
#         row['charts'] = [ 
#             {
#                 'title': 'מי פעיל/ה ואיפה',
#                 'description': '*ארגונים שדיווחו על כמה אזורי פעילות נספרים במחוזות השונים',
#                 'subcharts': [
#                     {
#                         'title': 'ארגונים: <span class="figure">{}</span>'.format(details['report'].get('total', {}).get('total_amount', 0)),
#                         'long_title': 'מספר הארגונים הפעילים בתחום {} לפי מחוז'.format(foad),
#                         'type': 'horizontal-barchart',
#                         'chart': {
#                             'values': [
#                                 dict(
#                                     label='<a href="//next.obudget.org/i/reports/ngo-district-report/{0}">{0}</a>'.format(x[0]),
#                                     value=x[1]
#                                 )
#                                 for x in details['report'].get('total', {}).get('association_activity_region_districts', [])
#                             ]
#                         }
#                     },
#                     {
#                         'title': 'בעלי אישור ניהול תקין: <span class="figure">{}</span>'.format(details['report'].get('proper_management', {}).get('total_amount', 0)),
#                         'long_title': 'מספר הארגונים הפעילים בתחום {} לפי מחוז'.format(foad),
#                         'type': 'horizontal-barchart',
#                         'chart': {
#                             'values': [
#                                 dict(
#                                     label='<a href="//next.obudget.org/i/reports/ngo-district-report/{0}">{0}</a>'.format(x[0]),
#                                     value=x[1]
#                                 )
#                                 for x in details['report'].get('proper_management', {}).get('association_activity_region_districts', [])
#                             ]
#                         }
#                     },
#                     {
#                         'title': 'בעלי סעיף 46: <span class="figure">{}</span>'.format(details['report'].get('has_article_46', {}).get('total_amount', 0)),
#                         'long_title': 'מספר הארגונים הפעילים בתחום {} לפי מחוז'.format(foad),
#                         'type': 'horizontal-barchart',
#                         'chart': {
#                             'values': [
#                                 dict(
#                                     label='<a href="//next.obudget.org/i/reports/ngo-district-report/{0}">{0}</a>'.format(x[0]),
#                                     value=x[1]
#                                 )
#                                 for x in details['report'].get('has_article_46', {}).get('association_activity_region_districts', [])
#                             ]
#                         }
#                     },
#                 ]
#             },
#             {
#                 'title': 'מי מקבל/ת כספי ממשלה, וכמה?',
#                 'long_title': 'אילו ארגונים בתחום {} מקבלים כספי ממשלה, וכמה?'.format(details['field_of_activity_display']),
#                 'subcharts': [
#                     {
#                         'title': 'סה״כ העברות כספי מדינה' + '<br/><span class="figure">{:,} ₪</span>'.format(details['income_total']),
#                         'type': 'adamkey',
#                         'chart': {
#                             'values': [
#                                 dict(
#                                     label='<a href="/i/{}">{}</a>'.format(x['doc_id'], x['name']),
#                                     amount=x['amount'],
#                                     amount_fmt='{:,} ₪'.format(x['amount']),
#                                 )
#                                 for x in details['income_list']
#                             ]
#                         }
#                     },
#                     {
#                         'title': 'סך התקשרויות ממשלתיות מדווחות' + '<br/><span class="figure">{:,} ₪</span>'.format(details['income_total_contracts']),
#                         'type': 'adamkey',
#                         'chart': {
#                             'values': [
#                                 dict(
#                                     label='<a href="/i/{}">{}</a>'.format(x['doc_id'], x['name']),
#                                     amount=x['amount'],
#                                     amount_fmt='{:,} ₪'.format(x['amount']),
#                                 )
#                                 for x in details['income_list_contracts']
#                             ]
#                         }
#                     },
#                     {
#                         'title': 'סך התמיכות הממשלתיות המדווחות' + '<br/><span class="figure">{:,} ₪</span>'.format(details['income_total_supports']),
#                         'type': 'adamkey',
#                         'chart': {
#                             'values': [
#                                 dict(
#                                     label='<a href="/i/{}">{}</a>'.format(x['doc_id'], x['name']),
#                                     amount=x['amount'],
#                                     amount_fmt='{:,} ₪'.format(x['amount']),
#                                 )
#                                 for x in details['income_list_supports']
#                             ]
#                         }
#                     },
#                 ]
#             },
#             {
#                 'title': 'במה מושקע הכסף הממשלתי?',
#                 'description': 'הנתונים המוצגים כוללים את העברות הכספי המתועדות במקורות המידע שלנו בכל השנים',
#                 'type': 'spendomat',
#                 'chart': {
#                     'data': spending_analysis
#                 }                
#             }
#         ]
#         row['others'] = [x for x in all_foas if x != foa]

#     elif row['key'].startswith('ngo-district-report'):
#         details = row['details']
#         district = details['district']
#         row['charts'] = [ 
#             {
#                 'title': 'מספר הארגונים הפעילים במחוז {} לפי תחום'.format(district),
#                 'subcharts': [
#                     {
#                         'title': 'סה״כ ארגונים באזור: <span class="figure">{}</span>'.format(details['report'].get('total', {}).get('count', 0)),
#                         'type': 'horizontal-barchart',
#                         'chart': {
#                             'values': [
#                                 dict(
#                                     label='<a href="//next.obudget.org/i/reports/ngo-activity-report/{0}">{0}</a>'.format(x[0]),
#                                     value=x[1]
#                                 )
#                                 for x in details['report'].get('total', {}).get('activities', [])
#                             ]
#                         }
#                     },
#                     {
#                         'title': 'סה״כ ארגונים עם אישור ניהול תקין: <span class="figure">{}</span>'.format(details['report'].get('proper_management', {}).get('count', 0)),
#                         'type': 'horizontal-barchart',
#                         'chart': {
#                             'values': [
#                                 dict(
#                                     label='<a href="//next.obudget.org/i/reports/ngo-activity-report/{0}">{0}</a>'.format(x[0]),
#                                     value=x[1]
#                                 )
#                                 for x in details['report'].get('proper_management', {}).get('activities', [])
#                             ]
#                         }
#                     },
#                     {
#                         'title': 'סה״כ ארגונים עם אישור 46: <span class="figure">{}</span>'.format(details['report'].get('has_article_46', {}).get('count', 0)),
#                         'type': 'horizontal-barchart',
#                         'chart': {
#                             'values': [
#                                 dict(
#                                     label='<a href="//next.obudget.org/i/reports/ngo-activity-report/{0}">{0}</a>'.format(x[0]),
#                                     value=x[1]
#                                 )
#                                 for x in details['report'].get('has_article_46', {}).get('activities', [])
#                             ]
#                         }
#                     },

#                 ]
#             },
#         ]
#         row['others'] = [x for x in all_districts if x != district]
#
#    return row


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
WITH a AS
  (SELECT jsonb_array_elements(association_activity_region_districts) AS district,
          association_field_of_activity,
          count(1) as cnt
   FROM guidestar_processed
   WHERE association_status_active
   group by 1, 2)
SELECT district,
       association_field_of_activity AS field_of_activity,
       rank
FROM
  (SELECT a.*,
          rank() OVER (PARTITION BY district
                       ORDER BY cnt DESC) AS rank
   FROM a) r
WHERE rank<=3"""
    results = engine.execute(foas_q)
    results = [dict(r) for r in results]
    ret = {}
    for r in results:
        ret.setdefault(r['district'], dict(totals=0, foas=[]))['foas'].append(r['field_of_activity'])
    
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
    where amount > 0
    group by 1
    order by 2 desc
    """
    results = engine.execute(q)
    results = [dict(r) for r in results]
    return results    


def add_main_page_report(res):
    yield from res
    row = dict(
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
                            amount_fmt='{:,} ₪'.format(x['amount']),
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
