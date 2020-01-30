import dataflows as DF


def filter_years(rows):
    return (row for row in rows if row.get('שנה') and row.get('סוג תקציב'))


def flow(*_):
    return DF.Flow(
        filter_years
    )
