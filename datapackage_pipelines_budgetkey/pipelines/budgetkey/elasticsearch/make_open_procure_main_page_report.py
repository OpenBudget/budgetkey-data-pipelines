import os
import json
import datetime
from sqlalchemy import create_engine, text

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines_budgetkey.common.format_number import format_number

engine = create_engine(os.environ['DPP_DB_ENGINE']).connect()

last_year = datetime.datetime.now().year - 1

# ב[שנה שעברה] רכשה המדינה סחורות ושירותים בכ-[סכום כספי]. 
# פעולות הרכש כללו [מספר מכרזים מרכזיים] מכרזים מרכזיים של מנהל הרכש
# וב-[מכרזים משרדיים] מכרזים משרדיים של [מספר משרדים שפרסמו מכרזים משרדיים].
# בנוסף, אושרו [מספר בקשות פטור ממכרז שאושרו] פעולות רכש בפטור ממכרז בשנה זו.

def get_single_result(query, **options):
    q = query.format(last_year=last_year, **options)
    results = engine.execute(text(q))
    results = [r._asdict()['x'] for r in results][0]
    return results

total_spending_q = """
SELECT sum(volume) as x
FROM contract_spending
WHERE date_part('year', order_date) = {last_year}
"""

tender_count_q = """
SELECT count({count_field}) as x 
FROM procurement_tenders_processed
WHERE 
(date_part('year', claim_date) = {last_year} or 
 date_part('year', start_date) = {last_year} or
 date_part('year', last_update_date) = {last_year})
AND tender_type = '{tender_type}'
"""

distinct_publisher = 'distinct publisher'

def count_tenders(tender_type, count_field):
    return get_single_result(tender_count_q,         
        count_field=count_field,
        tender_type=tender_type,
    )

def total_amount():
    return get_single_result(total_spending_q)

def add_main_page_report(res):
    yield from res
    row = dict(
        doc_id='reports/open-procure-main-page',
        key='open-procure-main-page',
        details=dict(
            year=last_year,
            total_amount=total_amount(),
            num_central=count_tenders('central', 1),
            num_office=count_tenders('office', 1),
            num_office_publishers=count_tenders('office', distinct_publisher),
            num_exemptions=count_tenders('exemptions', 1),
        ),
    )
    yield row

def process_resources(res_iter):
    first = next(res_iter)
    yield add_main_page_report(first)
    yield from res_iter


if __name__ == '__main__':
    params, dp, res_iter = ingest()
    spew(dp, process_resources(res_iter))
