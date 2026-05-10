import dataflows as DF

def add_hierarchy_columns(row):
    hierarchy = row.get('hierarchy')
    if not hierarchy:
        return row
    hierarchy = [*hierarchy, (row['code'], row['title'])]
    for code, title in hierarchy:
        code = code[2:]
        if len(code) == 2:
            row['code_level2'] = code
            row['title_level2'] = title
        elif len(code) == 4:
            row['code_level4'] = code
            row['title_level4'] = title
        elif len(code) == 6:
            row['code_level6'] = code
            row['title_level6'] = title
        elif len(code) == 8:
            row['code_level8'] = code
            row['title_level8'] = title
    return row

def flow(*_):
    return DF.Flow(
        DF.add_field('code_level2', 'string'),
        DF.add_field('code_level4', 'string'),
        DF.add_field('code_level6', 'string'),
        DF.add_field('code_level8', 'string'),
        DF.add_field('title_level2', 'string'),
        DF.add_field('title_level4', 'string'),
        DF.add_field('title_level6', 'string'),
        DF.add_field('title_level8', 'string'),
        add_hierarchy_columns
    )
