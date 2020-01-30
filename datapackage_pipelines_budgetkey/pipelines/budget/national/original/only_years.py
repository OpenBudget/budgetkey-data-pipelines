import dataflows as DF


def filter_years(rows):
    return (row for row in rows if 'שנה' in row)


def flow(*_):
    return DF.Flow(
        filter_years
    )
