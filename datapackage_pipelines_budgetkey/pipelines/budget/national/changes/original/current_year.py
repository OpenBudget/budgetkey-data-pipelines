import dataflows as DF

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines_budgetkey.processors.resource_without_excel_formulas_lib \
    import iter_resource_without_excel_formulas
from datapackage_pipelines_budgetkey.pipelines.budget.national.changes.current_year_urls \
    import get_current_year_urls

# (resource name, discovered-url key) for the current-year data files. The URLs
# are discovered from the gov.il page (see current_year_urls.py) rather than
# hardcoded, since they carry a weekly-changing date.
SOURCES = [
    ('changes_2026', 'approv_data'),
    ('pending_changes_2026', 'vaada_data'),
]

TABULATOR = {'encoding': 'windows-1255', 'headers': 4}


def data_resource(name, url):
    headers, rows = iter_resource_without_excel_formulas(url, {'tabulator': TABULATOR})
    schema = {'fields': [{'name': h, 'type': 'string'} for h in headers]}
    return rows, DF.update_resource(
        -1, name=name, path='data/%s.csv' % name, schema=schema, **{PROP_STREAMING: True}
    )


def flow(*_):
    urls = get_current_year_urls()
    steps = []
    for name, key in SOURCES:
        rows, update = data_resource(name, urls[key])
        steps.append(rows)
        steps.append(update)
    return DF.Flow(*steps)


if __name__ == '__main__':
    DF.Flow(flow(), DF.printer()).process()
