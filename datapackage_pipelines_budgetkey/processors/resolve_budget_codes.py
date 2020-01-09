import os
from sqlalchemy import create_engine
import dataflows as DF

query = """
WITH a AS
  (SELECT code,
          max(YEAR) AS year
   FROM raw_budget
   WHERE length(code)=10
   GROUP BY code)
SELECT year,
       code,
       title
FROM raw_budget
JOIN a USING (code,
              year)
ORDER BY code;
"""


def process_row(codes):
    def func(row):
        budget_codes = row.get('budget_codes', row.get('budget_code'))
        ret = []
        if budget_codes:
            if isinstance(budget_codes, str):
                budget_codes = [budget_codes]
            for code in budget_codes:
                code = codes.get(code)
                if code:
                    ret.append(dict(
                        code=code['code'],
                        year=code['year'],
                        title=code['title'],
                        doc_id='budget/{code}/{year}'.format(**code)
                    ))
        return ret
    return func


def flow(*_):
    engine = create_engine(os.environ['DPP_DB_ENGINE'])
    result = engine.execute(query)
    data = (dict(r) for r in result)
    codes = dict(
        (i['code'], i) for i in data
    )
    logging.info('GOT %d CODES', len(codes))
    return DF.Flow(
        DF.add_field('resolved_budget_codes', 'array', default=process_row(codes),
                     **{
                         'es:itemType': 'object',
                         'es:schema': dict(
                             code=dict(type='string', **{'es:keyword': True}),
                             year=dict(type='integer'),
                             title=dict(type='string'),
                         )
                        }), 
    )

