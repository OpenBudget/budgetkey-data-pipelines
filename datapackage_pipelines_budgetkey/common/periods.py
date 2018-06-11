import datetime

quarters_to_publication_dates = {
    '1': (0, 5, 15),
    '2': (0, 8, 15),
    '3': (0, 11, 15),
    '4': (1, 2, 15),
}
quarters_to_q_end_date = {
    '1': (0, 3, 31),
    '2': (0, 6, 30),
    '3': (0, 9, 30),
    '4': (0, 12, 31),
}


def convert_period(period, publication_date=True):
    year, period = period.split('-')
    try:
        assert int(year) > 2000
        assert int(period) > 0
        assert int(period) < 5
    except:
        return None
    if publication_date:
        qtd = quarters_to_publication_dates.get(period)
    else:
        qtd = quarters_to_q_end_date.get(period)
    assert qtd is not None
    overflow, month, day = qtd
    year = int(year) + overflow
    return datetime.date(year=year, month=month, day=day)
