import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).parents[2] / 'datapackage_pipelines_budgetkey/pipelines/budget/municipal/budget_files'
sys.path.insert(0, str(HERE))

import budget_file_analysis as a  # noqa: E402
import budget_file_store as store  # noqa: E402
import budget_report as report  # noqa: E402


def sheet(*header_rows, data):
    return [list(r) for r in header_rows] + [list(r) for r in data]


INCOMES_NEGATIVE = sheet(
    ['תקציב עיריית דוגמה לשנת 2017'],
    [],
    ['חשבון', 'שם חשבון', 'ביצוע 2015', 'תקציב 2016', 'תקציב 2017'],
    data=[
        [1111000100, 'ארנונה כללית', -224000000, -230000000, -237500000],
        [1122000220, 'אגרת שילוט', -4000000, -4250000, -4900000],
        [1312200420, 'השתתפות הזנה', -18000, -20000, -21000],
        [1614000750, 'עבודות קבלניות', 400000, 413000, 420000],
        [1741000752, 'אחזקת מתקני משחקים', 230000, 240000, 250000],
        [1766000110, 'ועדים מקומיים', 300000, 306000, 310000],
        [None, 'סה"כ', -227088000, -233311000, -241441000],
    ])


def test_detailed_sheet_with_title_rows():
    t = a.analyze_rows(INCOMES_NEGATIVE, 'תקציב', fiscal_year=2017)
    assert (t['kind'], t['header_row'], t['first_col'], t['code_col'], t['title_col']) == ('detailed', 2, 0, 0, 1)
    assert t['n_rows'] == 6
    assert [(v['kind'], v['year']) for v in t['value_columns']] == [
        ('executed', 2015), ('approved', 2016), ('approved', 2017)]
    assert t['expected_columns']
    assert (t['income_sign'], t['expense_sign']) == ('negative', 'positive')
    assert (t['units'], t['units_source']) == ('nis', 'magnitude')
    assert t['ready'] and t['issues'] == []


def test_thousands_from_header_and_positive_incomes():
    rows = sheet(
        ['הצעת תקציב 2024 (באלפי ש"ח)'],
        ['סעיף', 'פרק', 'סעיף תקציב', 'שם סעיף', 'משרות', 'תקציב 2023', 'הצעת תקציב 2024'],
        data=[[110, 11, 1111100110 + i, 'שורה %d' % i, 3, 2500 + i, 2600 + i] for i in range(4)]
        + [[420, 61, 1611000110 + i, 'הוצאה %d' % i, 1, -900 - i, -950 - i] for i in range(4)])
    t = a.analyze_rows(rows, 'גיליון1', fiscal_year=2024)
    assert (t['code_col'], t['title_col'], t['first_col']) == (2, 3, 2)
    # "סעיף", "פרק" and "משרות" are numbers but not budget values
    assert [v['col'] for v in t['value_columns']] == [5, 6]
    assert [v['kind'] for v in t['value_columns']] == ['approved', 'proposed']
    assert (t['units'], t['units_source']) == ('thousands', 'header')
    assert (t['income_sign'], t['expense_sign']) == ('positive', 'negative')


def test_thousands_from_magnitude():
    rows = sheet(['קוד', 'שם', 'תקציב 2025'],
                 data=[[1111000100 + i, 'שורה %d' % i, 120 + i * 37] for i in range(8)])
    t = a.analyze_rows(rows, 's', 2025)
    assert (t['units'], t['units_source']) == ('thousands', 'magnitude')


def test_decimal_amounts_are_not_codes():
    rows = sheet(['הכנסות', 'ביצוע 2019', 'תקציב 2020'],
                 data=[['ארנונה', 52781830.6, 51967526.4]] * 6)
    t = a.analyze_rows(rows, 'ריכוז', 2020)
    assert t['kind'] == 'summary' and t['issues'] == ['no-budget-codes'] and not t['ready']


def test_composite_codes():
    rows = sheet(['הכנסות'], ['סעיף', 'ספר', 'פרק', 'שם חשבון', 'תקציב 2022', 'תקציב 2023'],
                 data=[[100, 1, 111, 'ארנונה כללית', -501678000, -548961000],
                       [490, 1, 111, 'החזר אגרות', -19500, -18745],
                       [None, None, '111 סה"כ', None, -501697500, -548979745],
                       [100, 1, 112, 'אגרת שיטור', -14000000, -14200000],
                       [1110, 1, 115, 'הנחות לזכאים', -95800000, -95800000],
                       [120, 1, 116, 'היטלים', -300000, -310000]])
    t = a.analyze_rows(rows, 'הכנסות', 2023)
    assert t['code_col'] == 'composite:1+2+0' and 'composite-code' in t['issues']
    assert (t['n_rows'], t['first_col'], t['title_col']) == (5, 0, 3)
    assert t['income_sign'] == 'negative' and t['ready']
    assert a.composite_code([100, 1, 111], 1, 2, 0) == '1111000100'


def test_title_prefers_line_item_names():
    rows = sheet(['פרק', 'שם פרק משנה', 'כרטיס', 'שם כרטיס', 'תקציב 2024'],
                 data=[[61, 'משאבי אנוש', 1615000110 + i, 'פריט %d' % i, -10000 * (i + 1)] for i in range(6)])
    assert a.analyze_rows(rows, 's', 2024)['title_col'] == 3


@pytest.mark.parametrize('cell, expected', [
    ('1,234', 1234), ('(1,234)', -1234), ('1,234-', -1234), ('-5', -5), ('‏2,889,000‏', 2889000),
    ('שם', None), (None, None), (True, None), (12.5, 12.5),
])
def test_number(cell, expected):
    assert a.number(cell) == expected


PDF_TEXT = '''
                    הצעת תקציב לשנת 2023
      תקציב 2023           תקציב 2022 מעודכן       ביצוע 2021                 שם סעיף          סעיף
''' + '\n'.join(
    '      %s           %s       %s          ארנונה %d        %d' % (
        format(3000000 + i, ','), format(2900000 + i, ','), format(2800000, ','), i, 1111000110 + i)
    for i in range(6)) + '\n' + '\n'.join(
    '      (%s)           (%s)       (%s)          שכר %d        %d' % (
        format(500000 + i, ','), format(480000, ','), format(470000, ','), i, 1611000110 + i)
    for i in range(6))


def test_pdf_text_table():
    t = a.analyze_pdf_text(PDF_TEXT, pages=1, fiscal_year=2023)
    assert (t['kind'], t['n_rows']) == ('detailed', 12)
    assert [(v['kind'], v['year']) for v in t['value_columns']] == [
        ('approved', 2023), ('revised', 2022), ('executed', 2021)]
    assert (t['income_sign'], t['expense_sign'], t['units']) == ('positive', 'negative', 'nis')
    assert t['ready'] and t['expected_columns']


def test_scanned_pdf():
    t = a.analyze_pdf_text('\x0c' * 10, pages=10)
    assert t['kind'] == 'scanned' and t['issues'] == ['no-text-layer']


def test_csv(tmp_path):
    path = tmp_path / 'b.csv'
    lines = ['קוד;שם;תקציב 2025'] + ['%d;שורה %d;%d' % (1111000100 + i, i, 100000 * (i + 1)) for i in range(6)]
    path.write_bytes('\n'.join(lines).encode('cp1255'))
    summary, tables = a.analyze_file(str(path), 'csv', 2025)
    assert summary['ready_to_process'] and tables[0]['n_rows'] == 6


# --- storage -------------------------------------------------------------------

class FakeStore:
    s3 = False

    def __init__(self, root):
        self.root = root
        self.writes = []

    def get_path(self, name):
        return str(self.root / name)

    def urlfor(self, name):
        return 'https://s3.example/budgetkey-files/' + name

    def write(self, name, data=None, file_name=None, content_type=None):
        self.writes.append(name)
        path = Path(self.get_path(name))
        path.parent.mkdir(parents=True, exist_ok=True)
        if file_name:
            path.write_bytes(Path(file_name).read_bytes())
        else:
            path.write_text(data)


def write_xlsx(path, rows):
    import openpyxl
    wb = openpyxl.Workbook()
    for r in rows:
        wb.active.append(r)
    wb.save(path)


FILE = dict(authority_code='31', name='אופקים', file_url='https://muni.example.il/b.xlsx', format='xlsx',
            fiscal_year=2017)


def test_process_file_downloads_stores_and_reuses_analysis(tmp_path, monkeypatch):
    fake = FakeStore(tmp_path)
    monkeypatch.setattr(store, 'storage', lambda: fake)
    downloads = []

    def download(url, fmt, dest):
        downloads.append(url)
        write_xlsx(dest, INCOMES_NEGATIVE)
    monkeypatch.setattr(store, 'download', download)

    f, tables = store.process_file(dict(FILE))
    assert f['analysis_status'] == 'ok' and f['ready_to_process'] and f['units'] == 'nis'
    assert f['stored_url'].startswith('https://s3.example/budgetkey-files/municipal-budgets/31/')
    assert len(tables) == 1 and json.loads(tables[0]['value_columns'])[0]['kind'] == 'executed'
    assert len(fake.writes) == 2  # the file and its analysis

    f2, tables2 = store.process_file(dict(FILE))
    assert downloads == [FILE['file_url']] and len(fake.writes) == 2
    assert f2['ready_to_process'] and tables2 == tables


def test_process_file_rejects_html_pages(tmp_path, monkeypatch):
    fake = FakeStore(tmp_path)
    monkeypatch.setattr(store, 'storage', lambda: fake)

    class Resp:
        def raise_for_status(self):
            pass

        def iter_content(self, n):
            yield b'<!DOCTYPE html><html>not found</html>'
    monkeypatch.setattr(store.requests, 'get', lambda *args, **kw: Resp())
    f, tables = store.process_file(dict(FILE))
    assert f['analysis_status'] == 'not-a-xlsx' and tables == [] and fake.writes == []


# --- report --------------------------------------------------------------------

def test_report_ranks_machine_readable_years_first():
    pages = [dict(authority_code=c, name=n, website='https://%s.muni.il/' % c, status=s, authority_type='עירייה',
                  budget_page_url='https://%s.muni.il/budget/' % c if s == 'ok' else None)
             for c, n, s in [('1', 'שקופה', 'ok'), ('2', 'סרוקה', 'ok'), ('3', 'נעלמה', 'no-budget-page')]]
    files = [dict(authority_code='1', fiscal_year=y, format='xlsx', ready_to_process=True, file_url='https://1/%d' % y,
                  table_kind='detailed', analysis_status='ok', doc_type='budget') for y in (2024, 2025)] + \
            [dict(authority_code='2', fiscal_year=2025, format='pdf', table_kind='scanned', analysis_status='ok',
                  file_url='https://2/2025', doc_type='budget')]
    html = report.render(pages, files)
    assert html.index('שקופה') < html.index('סרוקה') < html.index('נעלמה')
    assert 'לא נמצא דף תקציב' in html and 'XLS ✓' in html and 'סרוק' in html
