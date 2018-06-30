import os
import sqlalchemy
import itertools

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

if __name__ == '__main__':
    params, dp, res_iter = ingest()

    dp['resources'].append(
        dict(
            name='subs', 
            path='subs.csv',
            schema=dict(
                fields=[
                    dict(name='email', type='string'),
                    dict(name='items', type='array')
                ]
            )
        )
    )
    dp['resources'][0][PROP_STREAMING] = True
    
    e = sqlalchemy.create_engine(os.environ['PRIVATE_DATABASE_URL'])
    r = map(dict,
            e.execute("""
select email, title, url, properties 
from items join lists on(items.list_id=lists.id) 
join users on(lists.user_id=users.id) 
where lists.name='searches' 
order by email
"""
        ))

    r = (dict(email=email, items=list(items))
         for email, items in itertools.groupby(r, lambda x: x['email']))
    spew(dp, [r])




