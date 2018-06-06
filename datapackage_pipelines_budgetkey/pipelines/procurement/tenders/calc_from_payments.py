from datapackage_pipelines.wrapper import process

def process_row(row, *_):
    contracts = row['contracts']
    row['contract_volume'] = sum(c.get('volumne', 0) for c in contracts)
    row['contract_executed'] = sum(c.get('executed', 0) for c in contracts)
    return row
    

def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        dict(
            name='contract_volume',
            type='number',
        ),
        dict(
            name='contract_executed',
            type='number'
        )
    ])
    return dp


if __name__=='__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)