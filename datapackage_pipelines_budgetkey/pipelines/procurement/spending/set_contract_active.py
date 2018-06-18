import datetime

from datapackage_pipelines.wrapper import process

from datapackage_pipelines_budgetkey.common.periods import convert_period

now = datetime.datetime.now().date()
    

def process_row(row, *_):
    active = True
    if row['end_date']:
        active = now < row['end_date']
    else:
        last_activity = [
            convert_period(p['timestamp'])
            for p in row['payments']
        ]
        last_activity = [x for x in last_activity if x is not None]
        if len(last_activity) > 0:
            last_activity = max(last_activity)
            if (now - last_activity).days > 180:
                active = False
    row['contract_is_active'] = active
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append(dict(
      name='contract_is_active',
      type='boolean'  
    ))
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
