from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    if row['year_paid'] == 'אינו מוקצה':
        row['year_paid'] = ''
    return row


process(process_row=process_row)