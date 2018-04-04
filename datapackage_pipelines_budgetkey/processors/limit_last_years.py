import datetime

from datapackage_pipelines.wrapper import process

min_year = (datetime.datetime.now() - datetime.timedelta(weeks=26)).year - 3

def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    year = row[parameters['year_column']]
    target_col = parameters['target_column']
    if year is not None and year >= min_year:
        if parameters.get('as_bool'):
            row[target_col] = True
        else:
            row[target_col] = row[parameters['amount_column']]
    else:
        if parameters.get('as_bool'):
            row[target_col] = False
        else:
            row[target_col] = 0
    return row


def modify_datapackage(datapackage, parameters, _):
    datapackage['resources'][0]['schema']['fields'].append({
        'name': parameters['target_column'],
        'type': 'boolean' if parameters.get('as_bool') else 'number' 
    })
    return datapackage


if __name__=="__main__":
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)