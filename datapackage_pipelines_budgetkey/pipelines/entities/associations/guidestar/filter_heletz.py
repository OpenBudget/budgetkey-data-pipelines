from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    if spec['name'] == 'company-registry':
        if row['Company_Name']:
            if 'חל"צ' in row['Company_Name']:
                return row
            if 'חברה לתועלת הציבור' in row['Company_Name']:
                return row
    else:
        return row


process(process_row=process_row)