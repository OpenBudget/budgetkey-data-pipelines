import datetime
import dataflows as DF

yesterday = datetime.datetime.now() - datetime.timedelta(days=2)
yesterday = yesterday.isoformat()


def flow(*_):
    return DF.Flow(
        DF.filter_rows(lambda row: row.get('__seen_date') and row.get('__seen_date') > yesterday)
    )