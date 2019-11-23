import datetime
import dataflows as DF

now = datetime.datetime.now().date().isoformat()


def flow(*_):
    return DF.Flow(
        DF.add_field('__seen_date', 'string', now)
    )