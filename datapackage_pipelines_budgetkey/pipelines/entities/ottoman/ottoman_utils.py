def process_row(row, *_):
    if len(row['id']) < 9:
        return None
    row['id'] = row['id'][:9]
    row['name'] = row['name'].strip(' \n\t.')
    row['address'] = row['address'].strip() if row['address'] else None
    return row
