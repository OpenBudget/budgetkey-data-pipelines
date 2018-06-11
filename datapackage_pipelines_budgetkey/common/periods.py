import datetime

quarters_to_dates = {
    '1': (0, 5),
    '2': (0, 8),
    '3': (0, 11),
    '4': (1, 2),
}
def convert_period(period):
    year, period = period.split('-')
    try:
        assert int(year) > 2000
        assert int(period) > 0
        assert int(period) < 5
    except:
        return None
    qtd = quarters_to_dates.get(period)
    assert qtd is not None
    overflow, month = qtd
    year = int(year) + overflow
    return datetime.date(year=year, month=month, day=15)
