import datetime

from datapackage_pipelines.wrapper import process

now = datetime.datetime.now().date()
quarters_to_dates = {
    '1': (0, 5),
    '2': (0, 8),
    '3': (0, 11),
    '4': (1, 2),
}
def convert(year, period):
    period = str(period)
    qtd = quarters_to_dates.get(period)
    if qtd is None:
        return None
    overflow, month = qtd
    year = int(year) + overflow
    return datetime.date(year=year, month=month, day=15)
    

def process_row(row, *_):
    active = True
    if row['end_date']:
        active = now < row['end_date']
    else:
        last_activity = [
            convert(p['year'], p['period'])
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
