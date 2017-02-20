from datapackage_pipelines.wrapper import process


def fix_bad_values(row, row_index,
                   resource_descriptor, resource_index,
                   parameters, stats):
    for k, v in row.items():
        if v in ['..']:
            row[k] = ''
    return row


process(process_row=fix_bad_values)