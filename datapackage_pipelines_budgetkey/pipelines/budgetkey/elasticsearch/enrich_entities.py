from datapackage_pipelines.wrapper import process
from sqlalchemy import create_engine
import os
import logging
from decimal import Decimal


engine = create_engine(os.environ['DPP_DB_ENGINE'])


class Enricher:

    def __init__(self, descriprtion, kind_filter, key_fields, target_fields, query):
        logging.info('Initializing %s', descriprtion)
        self.key_fields = key_fields
        self.target_fields = target_fields
        self.kind_filter = kind_filter
        self.descriprtion = descriprtion
        result = engine.execute(query)
        data = list(dict(r) for r in result)
        self.data = dict(
            (tuple(x.pop(k) for k in self.key_fields), self.normalize(x)) for x in data
        )
        logging.info('Get %d results', len(self.data))
        self.seen = self.filtered = self.modified = 0

    def normalize(self, r):
        for k, v in r.items():
            if isinstance(v, Decimal):
                r[k] = float(v)
        return r

    def modify_datapackage(self, dp):
        dp['resources'][0]['schema']['fields'].extend(self.target_fields)

    def get(self, r, f):
        if f in ('id', ):
            return r[f]
        else:
            return r.get('details', {}).get(f)

    def process_row(self, row):
        self.seen += 1
        if self.kind_filter is None or row['kind'] == self.kind_filter:
            self.filtered += 1
            details = row.get('details', {})
            key = tuple(self.get(row, f) for f in self.key_fields)
            enrich = self.data.get(key)
            if enrich is not None:
                self.modified += 1
                details.update(enrich)
            row['details'] = details

    def stats(self):
        return '{}: {} seen, {} filtered, {} modified'.format(self.descriprtion, self.seen, self.filtered, self.modified)


ENRICHERS = [
    Enricher('81066: Associations - unique in region and field',
        'association', ('id', ), 
        [{
            'name': 'unique_activity_regions',
            'type': 'array',
            'es:itemType': 'string'
        }],
        """
WITH a AS
  (SELECT jsonb_array_elements(association_activity_region_list) AS activity_region,
          association_field_of_activity AS field_of_activity,
          array_agg(id) AS ids
   FROM guidestar_processed
   WHERE association_status_active IS TRUE
   GROUP BY 1,
            2)
SELECT ids[1] AS id,
       array_agg(activity_region) AS unique_activity_regions
FROM a
WHERE array_length(ids, 1)=1
GROUP BY 1
"""
    ),
    Enricher('81069: Associations - unique in district and field',
        'association', ('id', ), 
        [{
            'name': 'unique_activity_districts',
            'type': 'array',
            'es:itemType': 'string'
        }],
        """
WITH a AS
  (SELECT jsonb_array_elements(association_activity_region_districts) AS activity_region,
          association_field_of_activity AS field_of_activity,
          array_agg(id) AS ids
   FROM guidestar_processed
   WHERE association_status_active IS TRUE
   GROUP BY 1,
            2)
SELECT ids[1] AS id,
       array_agg(activity_region) AS unique_activity_districts
FROM a
WHERE array_length(ids, 1)=1
GROUP BY 1
"""
    ),
    Enricher('81127: Association Rank of Yearly turnover per activity field',
        'association', ('id', ),
        [{
            'name': 'rank_of_yearly_turnover_in_field',
            'type': 'integer',
        }],        
        """
SELECT id, rank as rank_of_yearly_turnover_in_field
FROM
  (SELECT id,
          rank() OVER (PARTITION BY association_field_of_activity
                       ORDER BY association_yearly_turnover DESC) AS rank
   FROM guidestar_processed
   WHERE association_status_active
     AND association_yearly_turnover>0) r
WHERE RANK <= 10
"""
    ),
    Enricher('81160: Association Rank of Yearly turnover per activity field per district',
        'association', ('id', ),
        [{
            'name': 'has_highest_turnover_in_district',
            'type': 'boolean'
        }],
        """
WITH a AS
  (SELECT id,
          association_field_of_activity,
          association_yearly_turnover,
          jsonb_array_elements(association_activity_region_districts) AS district,
          association_activity_region_districts
   FROM guidestar_processed
   WHERE association_status_active
     AND association_yearly_turnover>0)
SELECT id,
       district is not null as has_highest_turnover_in_district
FROM
  (SELECT a.*,
          rank() OVER (PARTITION BY association_field_of_activity,
                                    district
                       ORDER BY association_yearly_turnover DESC) AS rank
   FROM a) r
WHERE RANK = 1
  AND jsonb_array_length(association_activity_region_districts)=1
"""
    ),
    Enricher('81075: Association median turnovers by field of activity',
        'association', ('field_of_activity', ),
        [
            {
                'name': 'median_turnover_in_field_of_activity',
                'type': 'number'
            }
        ],     
        """
WITH a AS
  (SELECT association_field_of_activity AS foa,
          association_yearly_turnover AS turnover
   FROM guidestar_processed
   WHERE association_status_active
     AND association_yearly_turnover>0
   ORDER BY 2),
     b AS
  (SELECT foa,
          array_agg(turnover) AS turnovers
   FROM a
   GROUP BY 1)
SELECT foa as field_of_activity,
       turnovers[ceil(array_length(turnovers, 1))] AS median_turnover_in_field_of_activity
FROM b
"""
    ),
    Enricher('81170: Association Rank of employees per activity field',
    'association', ('id', ),
    [
        {
            'name': 'rank_of_employees_in_field_of_activity',
            'type': 'integer'
        }
    ],
    """
WITH a AS
  (SELECT id,
          association_field_of_activity,
          association_num_of_employees
   FROM guidestar_processed
   WHERE association_status_active
     AND association_num_of_employees>0)
SELECT id,
       rank as rank_of_employees_in_field_of_activity
FROM
  (SELECT a.*,
          rank() OVER (PARTITION BY association_field_of_activity
                       ORDER BY association_num_of_employees DESC) AS rank
   FROM a) r
WHERE rank<=3
"""),
    Enricher('81161: Association Rank of volunteers per activity field',
    'association', ('id', ),
    [
        {
            'name': 'rank_of_volunteers_in_field_of_activity',
            'type': 'integer'
        }
    ],
    """
WITH a AS
  (SELECT id,
          association_field_of_activity,
          association_num_of_volunteers
   FROM guidestar_processed
   WHERE association_status_active
     AND association_num_of_volunteers>0)
SELECT id,
       rank as rank_of_volunteers_in_field_of_activity
FROM
  (SELECT a.*,
          rank() OVER (PARTITION BY association_field_of_activity
                       ORDER BY association_num_of_volunteers DESC) AS rank
   FROM a) r
WHERE rank<=3
"""),
    Enricher('81171: Association Rank of registration_date per activity field',
    'association', ('id', ),
    [
        {
            'name': 'is_oldest_org_in_field_of_activity',
            'type': 'boolean'
        }
    ],
    """
WITH a AS
  (SELECT id,
          association_title AS title,
          association_field_of_activity,
          association_registration_date
   FROM guidestar_processed
   WHERE association_status_active)
SELECT id,
       TRUE AS is_oldest_org_in_field_of_activity
FROM
  (SELECT a.*,
          rank() OVER (PARTITION BY association_field_of_activity
                       ORDER BY association_registration_date ASC) AS rank
   FROM a) r
WHERE rank=1
"""),
    Enricher('81173: Association Rank of registration_date per activity field and district',
    'association', ('id', ),
    [
        {
            'name': 'districts_where_oldest_org_in_field_of_activity',
            'type': 'array',
            'es:itemType': 'string'
        }
    ],
    """
WITH a AS
  (SELECT id,
          association_title AS title,
          association_field_of_activity,
          jsonb_array_elements(association_activity_region_districts) as district,
          association_registration_date
   FROM guidestar_processed
   WHERE association_status_active)
SELECT id,
       array_agg(district) as districts_where_oldest_org_in_field_of_activity
FROM
  (SELECT a.*,
          rank() OVER (PARTITION BY association_field_of_activity, district
                       ORDER BY association_registration_date ASC) AS rank
   FROM a) r
WHERE rank=1
group by id
"""),
    Enricher('81262: Association Rank of received moneys per activity field',
    'association', ('id', ),
    [
        {
            'name': 'rank_of_overall_recipient_in_field_of_activity',
            'type': 'integer'
        }
    ],
    """
WITH a AS
  (SELECT id,
          association_field_of_activity,
          received_amount AS amount
   FROM guidestar_processed
   JOIN entities_processed USING (id)
   WHERE association_status_active
     AND received_amount>0)
SELECT id,
       rank as rank_of_overall_recipient_in_field_of_activity
FROM
  (SELECT a.*,
          rank() OVER (PARTITION BY association_field_of_activity
                       ORDER BY amount DESC) AS rank
   FROM a) r
WHERE rank<=10
"""),
    Enricher('81257: Association Rank of received contracts per activity field',
    'association', ('id', ),
    [
        {
            'name': 'rank_of_contract_recipient_in_field_of_activity',
            'type': 'integer'
        }
    ],
    """
WITH a AS
  (SELECT id,
          association_field_of_activity,
          received_amount_contracts AS amount
   FROM guidestar_processed
   JOIN entities_processed USING (id)
   WHERE association_status_active
     AND received_amount_contracts>0)
SELECT id,
       rank as rank_of_contract_recipient_in_field_of_activity
FROM
  (SELECT a.*,
          rank() OVER (PARTITION BY association_field_of_activity
                       ORDER BY amount DESC) AS rank
   FROM a) r
WHERE rank<=10
"""),
    Enricher('81254: Association Rank of received supports per activity field',
    'association', ('id', ),
    [
        {
            'name': 'rank_of_supports_recipient_in_field_of_activity',
            'type': 'integer'
        }
    ],
    """
WITH a AS
  (SELECT id,
          association_field_of_activity,
          received_amount_supports AS amount
   FROM guidestar_processed
   JOIN entities_processed USING (id)
   WHERE association_status_active
     AND received_amount_supports>0)
SELECT id,
       rank as rank_of_supports_recipient_in_field_of_activity
FROM
  (SELECT a.*,
          rank() OVER (PARTITION BY association_field_of_activity
                       ORDER BY amount DESC) AS rank
   FROM a) r
WHERE rank<=10
"""),
    Enricher('81277: Unique contracts for entities',
    None, ('id', ),
    [
        {
            'name': 'sole_government_procurer_contract_count',
            'type': 'integer'
        },
        {
            'name': 'sole_government_procurer_name',
            'type': 'string'
        },
        {
            'name': 'sole_government_procurer_executed',
            'type': 'number'
        },
    ],
    """
WITH a AS
  (SELECT id,
          count(1) AS contract_count,
          array_agg(DISTINCT publisher_name) AS publishers,
          sum(executed) AS executed
   FROM entities
   JOIN contract_spending ON (id=entity_id)
   WHERE executed>0
   GROUP BY 1)
SELECT id,
       contract_count as sole_government_procurer_contract_count,
       publishers[1] as sole_government_procurer_name,
       executed as sole_government_procurer_executed
FROM a
WHERE array_length(publishers, 1)=1
"""),
    Enricher('81278: Unique supports for entities',
    None, ('id', ),
    [
        {
            'name': 'sole_government_supporter_contract_count',
            'type': 'integer'
        },
        {
            'name': 'sole_government_supporter_name',
            'type': 'string'
        },
        {
            'name': 'sole_government_supporter_paid',
            'type': 'number'
        },
    ],
    """
WITH a AS
  (SELECT id,
          count(DISTINCT (year_requested, budget_code)) AS support_count,
          array_agg(DISTINCT supporting_ministry) AS ministries,
          sum(amount_total) AS amount
   FROM entities
   JOIN raw_supports ON (id=entity_id)
   WHERE amount_total>0
   GROUP BY 1)
SELECT id,
       support_count as sole_government_supporter_contract_count,
       ministries[1] as sole_government_supporter_name,
       amount as sole_government_supporter_paid
FROM a
WHERE array_length(ministries, 1)=1
"""),
]


def process_row(row, *_):
    for e in ENRICHERS:
        e.process_row(row)
    return row


def modify_datapackage(dp, *_):
    for e in ENRICHERS:
        e.modify_datapackage(dp)
    return dp


if __name__ == "__main__":
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
    for e in ENRICHERS:
        logging.info('%r', e.stats())
