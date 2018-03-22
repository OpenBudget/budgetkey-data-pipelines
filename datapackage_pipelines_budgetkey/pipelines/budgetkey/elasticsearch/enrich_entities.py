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
        result = engine.execute(query)
        data = list(dict(r) for r in result)
        self.data = dict(
            (tuple(x.pop(k) for k in self.key_fields), self.normalize(x)) for x in data
        )
        logging.info('Get %d results', len(self.data))

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
        if self.kind_filter is None or row['kind'] == self.kind_filter:
            details = row.get('details', {})
            key = tuple(self.get(row, f) for f in self.key_fields)
            enrich = self.data.get(key)
            if enrich is not None:
                details.update(enrich)
            row['details'] = details


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
