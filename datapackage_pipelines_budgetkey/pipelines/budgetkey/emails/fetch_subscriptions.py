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
    
    e = sqlalchemy.create_engine(os.environ['PRIVATE_DATABASE_URL']).connect()
    r = map(lambda x: x._asdict(),
            e.execute(sqlalchemy.text("""
with a as (select users.email as email, items.title, url, items.properties as properties, max(send_time) as last_send_time
from items join lists on(items.list_id=lists.id) 
join users on(lists.user_id=users.id) 
left join sendlog on (sendlog.email=users.email)
where lists.name='searches'
group by 1,2,3,4)
select * from a 
where (last_send_time is null or current_timestamp > last_send_time + interval '5 days')
order by email
""")))

    r = (dict(email=email, items=list(items))
         for email, items in itertools.groupby(r, lambda x: x['email']))
    spew(dp, [r])
