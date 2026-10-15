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
        [1811000110, 'שכר חינוך', 227088000, 233311000, 241441000],
        [None, 'סה"כ', 0, 0, 0],
    ])


def test_detailed_sheet_with_title_rows():
    t = a.analyze_rows(INCOMES_NEGATIVE, 'תקציב', fiscal_year=2017)
    assert (t['kind'], t['header_row'], t['first_col'], t['code_col'], t['title_col']) == ('detailed', 2, 0, 0, 1)
    assert t['n_rows'] == 7
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
    assert len(fake.writes) == 3  # the file, its rows and its analysis

    f2, tables2 = store.process_file(dict(FILE))
    assert downloads == [FILE['file_url']] and len(fake.writes) == 3
    assert f2['ready_to_process'] and tables2 == tables

    rows = store.read_rows(f2)
    assert len(rows) == 7 * 3
    assert f2['balance'] == 1.0
    assert rows[0] == dict(table_index='0', sheet='Sheet', row='4', code='1111000100', title='ארנונה כללית',
                           column='0', kind='executed', year='2015', header='ביצוע 2015', value='-224000000')


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


def test_report_shows_every_year_and_file():
    pages = [dict(authority_code=c, name=n, website=None, status='ok', budget_page_url=None)
             for c, n in [('1', 'ותיקה'), ('2', 'חדשה')]]
    pdf = dict(format='pdf', table_kind='detailed', analysis_status='ok', doc_type='budget')
    # Twenty years of PDFs outrank three recent spreadsheets; old years and undated files are listed too
    files = [dict(pdf, authority_code='1', fiscal_year=y, file_url='https://1/%d' % y) for y in range(2003, 2023)] + \
            [dict(pdf, authority_code='1', fiscal_year='', file_url='https://1/undated')] + \
            [dict(authority_code='2', fiscal_year=y, format='xlsx', ready_to_process=True, analysis_status='ok',
                  file_url='https://2/%d/%d' % (y, i), doc_type='budget') for y in (2024, 2025, 2026) for i in range(6)]
    html = report.render(pages, files)
    assert '>2003</th>' in html and '>ללא שנה</th>' in html
    assert 'https://1/2003' in html and 'https://1/undated' in html and 'https://2/2026/5' in html
    top = html[html.index('הרשויות השקופות ביותר'):]
    assert top.index('ותיקה') < top.index('חדשה')


def test_detailed_sheet_rows():
    t = a.analyze_rows(INCOMES_NEGATIVE, 'תקציב', fiscal_year=2017)
    assert len(t['rows']) == 7
    assert t['rows'][0] == dict(row=4, code='1111000100', title='ארנונה כללית',
                                values=[-224000000, -230000000, -237500000])


def word(text, x, y, width=20):
    return dict(text=text, x=x, x0=x - width / 2, x1=x + width / 2, y=y)


def test_pdf_values_are_placed_under_their_columns():
    # A right-to-left table: values on the left, then the title, then the code; one row has gaps
    page = [word('2023', 90, 10, 16), word('תקציב', 112, 10, 22),
            word('2022', 180, 10, 16), word('תקציב', 202, 10, 22), word('מעודכן', 154, 10, 24),
            word('ביצוע', 300, 10, 22), word('2021', 278, 10, 16)]
    for i in range(6):
        y = 30 + i * 12
        page += [word('3,000,00%d' % i, 100, y), word('2,900,000', 200, y), word('2,800,000', 290, y),
                 word('ארנונה %d' % i, 400, y, 40), word(str(1111000110 + i), 500, y, 40)]
    page += [word('962,000', 200, 110), word('ארנונה נוספת', 400, 110, 50), word('1111100110', 500, 110, 40)]
    columns, rows = a.extract_pdf_table([page])
    assert [(c['kind'], c['year']) for c in columns] == [('approved', 2023), ('revised', 2022), ('executed', 2021)]
    assert rows[0] == dict(row='page 1', code='1111000110', title='ארנונה 0', values=[3000000, 2900000, 2800000])
    assert rows[-1]['values'] == [None, 962000, None]


def test_pdf_numbers_at_the_end_of_a_title_are_not_values():
    # Ofakim 2018: a licence number ends the title, right beside the value column it would otherwise fill
    page = [word('2018', 100, 10, 16), word('תקציב', 122, 10, 22)]
    for i in range(4):
        page += [word(str(i + 1), 100, 30 + i * 12, 6), word('רכב %d' % i, 200, 30 + i * 12, 30),
                 word(str(1741000733 + i * 1000), 300, 30 + i * 12, 40)]
    page += [word('1', 100, 90, 6), word('9292600', 123, 90, 37), word('ניגררת', 155, 90, 28),
             word('עגלה', 185, 90, 23), word('1741006733', 300, 90, 40)]
    columns, rows = a.extract_pdf_table([page])
    assert [(c['kind'], c['year']) for c in columns] == [('approved', 2018)]
    assert rows[-1] == dict(row='page 1', code='1741006733', title='עגלה ניגררת 9292600', values=[1])


def test_budget_lines_normalize_units_and_signs(monkeypatch):
    import budget_files_scraper as scraper
    files = [dict(authority_code='31', entity_id='500200316', name='אופקים', fiscal_year=2024, doc_type='budget',
                  format=fmt, ready_to_process=True, file_url='https://x/%s' % fmt, stored_url='s3://%s' % fmt)
             for fmt in ('pdf', 'xlsx')]
    tables = [dict(file_url='https://x/xlsx', table_index=0, ready=True, units='thousands', income_sign='negative',
                   expense_sign='positive', duplicate_columns=[1],
                   value_columns=json.dumps([dict(kind='approved', year=2024), dict(kind='approved', year=2024,
                                                                                    duplicate=True)]))]
    rows = [dict(table_index='0', sheet='s', row='5', code=code, title='t', column=column, kind='approved',
                 year='2024', header='תקציב 2024', value=value)
            for code, column, value in [('1111000100', '0', '-250'), ('1614000750', '0', '40'), ('1614000750', '1', '7')]]
    monkeypatch.setattr(store, 'read_rows', lambda f: rows if f['format'] == 'xlsx' else pytest.fail('PDF read'))
    lines = list(scraper.budget_lines(files, tables))
    # The PDF copy of the same budget is skipped, and so is the ambiguous duplicate column
    assert [(l['direction'], l['amount'], l['value'], l['format']) for l in lines] == [
        ('income', 250000, -250, 'xlsx'), ('expense', 40000, 40, 'xlsx')]
    assert lines[0]['chapter'] == '11' and lines[0]['sign_normalized']


def test_unbalanced_budget_is_not_ready(tmp_path):
    path = tmp_path / 'b.csv'
    lines = ['קוד,שם,תקציב 2025', '1111000100,ארנונה,1000000', '1112000100,אגרות,200000', '1113000100,היטלים,300000',
             '1611000100,שכר,400000', '1612000100,רכש,100000']
    path.write_text('\n'.join(lines), encoding='utf-8')
    summary, tables = a.analyze_file(str(path), 'csv', 2025)
    assert tables[0]['ready'] and not summary['ready_to_process']
    assert summary['balance'] == 3 and 'unbalanced' in summary['issues']


def test_slash_codes():
    rows = sheet(['פרק', 'סעיף', 'סעיף תקציב', 'שם סעיף', 'תקציב חדש 2026', 'תקן חדש 2026', 'הוצאה בפועל 2024'],
                 data=[[11000, 121, '11000/121/8', 'גביה שוטפת', 1320000000, 0, 1095136150]] * 3
                 + [[61111, 124, '61111/124/2', 'החזר הוצאות', 1320000000, 2, 1095136150]] * 3)
    t = a.analyze_rows(rows, 'הכנסות', 2026)
    assert (t['code_col'], t['title_col']) == (2, 3)
    assert [(v['kind'], v['year']) for v in t['value_columns']] == [('approved', 2026), ('executed', 2024)]
    assert a.chapter_kind('11000/121/8') == 'income' and a.chapter_of('61111/124/2') == '61'


def columns(*kind_years):
    return [dict(kind=k, year=y) for k, y in kind_years]


def headed(*headers):
    return [a.describe_value_column(h) for h in headers]


@pytest.mark.parametrize('fiscal_year, headers, expected', [
    # Reviewed by hand: (budget year, the value column headers, the three budget columns among them)
    (2025, ['הצעת תקציב 2025'], {0: 'proposed'}),  # Kiryat Bialik
    (2024, ['תקציב 2024'], {0: 'proposed'}),  # Bat Yam, Ramla: a lone column is the proposal
    (2021, ['ביצוע 2019', 'תקציב 2020', 'ביצוע 2020 (לא סופי)', 'תקציב 2021'],  # Kiryat Shmona
     {0: 'executed', 1: 'approved', 3: 'proposed'}),
    (2024, ['תקציב 2024', 'ביצוע 2023 (לא מבוקר)', 'תקציב  2023'], {0: 'proposed', 2: 'approved'}),  # Kfar Saba
    (2025, ['תקציב 2024 מקורי*', 'אומדן 2024', 'תקציב 2025'], {2: 'proposed'}),  # Hadera
    (2025, ['תקציב 2025 מאושר', 'תקציב מעודכן שוטף 2024', 'שינוי', 'תקציב 2024 מאושר'],  # Rishon LeZion
     {0: 'proposed', 1: 'approved'}),
    (2024, ['מחלקה', 'תקציב מעודכן', 'ביצוע מצטבר 2', 'בקשת מחלקה', 'בסיס 2024', 'מותנה 2024',  # Ashdod
            'תקציב 2024 כולל מותנה'], {1: 'approved', 6: 'proposed'}),
    (2022, ['תקציב חדש 2022', 'תקציב ישן 2021', 'הוצאה בפועל 2020'],  # Tel Aviv
     {0: 'proposed', 1: 'approved', 2: 'executed'}),
    (2025, ['ביצוע 2023', 'תקציב 2024', 'הצעת תקציב 2025'], {0: 'executed', 1: 'approved', 2: 'proposed'}),  # Haifa
    # Columns derived from the budget are never one of its three columns
    (2024, ['אחוז השינוי מול 2023', 'הצעה\n2024', 'הצעה\n2023', 'ביצוע\n2022'], {1: 'proposed', 3: 'executed'}),  # Holon
    (2013, ['הצעה 2013 %', 'הצעה 2013'], {1: 'proposed'}),  # Gush Etzion
    (2020, ['שינוי מתקציב 2019', 'תקציב 2020'], {1: 'proposed'}),  # Maghar
    (2023, ['תקציב 2022 שינוי 1', 'תקציב 2023'], {0: 'approved', 1: 'proposed'}),  # an amended budget is a budget
    # Base plus conditional is the total; the final figure is the latest
    (2022, ['בקשת מחלקה 2022', 'בסיס לתקציב 2022', 'מותנה 2022', 'תקציב כולל מותנה 2022', 'בסיס+מותנה 2021'],  # Ashdod
     {3: 'proposed', 4: 'approved'}),
    (2019, ['תקציב 2019', 'אומדן 2018', 'סופי 2018', 'מקורי 2018', 'ביצוע 2017'],  # Herzliya
     {0: 'proposed', 2: 'approved', 4: 'executed'}),
    (2019, ['תקציב 2019', '2018', 'סופי 2018'], {0: 'proposed', 2: 'approved'}),
    (2021, ['תקציב 2021', 'תקציב 2020', 'סופי'], {0: 'proposed', 1: 'approved'}),  # an undated final isn't dated by it
    (2018, ['הצעת מעודכנת  2017', 'תקציב 2018'], {0: 'approved', 1: 'proposed'}),  # Pasuta: updated, not a proposal
])
def test_budget_columns(fiscal_year, headers, expected):
    assert a.budget_columns(headed(*headers), fiscal_year) == expected
    assert a.expected_columns(headed(*headers), fiscal_year) == (len(expected) == 3)


def test_budget_year_comes_from_the_columns():
    cols = [columns(('approved', 2018), ('approved', 2017), ('executed', 2016))]
    assert a.budget_year(cols, 2017, 2027) == 2018
    assert a.budget_year(cols, 2018, 2027) == 2018
    assert a.budget_year([columns(('approved', 2022))], 2021, 2027) == 2021
    assert a.budget_year(cols, None, 2027) == 2018
    assert a.budget_year([headed('ביצוע', 'תקציב 2024', 'הצעת תקציב 2025')], None, 2027) == 2025
    # Only a year off: a multi-year table doesn't move a budget around
    assert a.budget_year([columns(*(('approved', y) for y in range(2019, 2030)))], 2023, 2027) == 2023


def test_files_are_flagged_and_relabelled_by_their_columns():
    f = dict(doc_type='budget', fiscal_year=2017, issues=None, expected_columns=False)
    result = dict(tables=[dict(ready=True, value_columns=columns(('approved', 2018), ('approved', 2017),
                                                                 ('executed', 2016)))])
    store.check_budget_columns(f, result)
    assert (f['fiscal_year'], f['fiscal_year_source']) == (2018, 'columns')
    assert f['expected_columns'] and f['issues'] is None and result['tables'][0]['expected_columns']

    f = dict(doc_type='budget', fiscal_year=2022, issues='units-unknown', expected_columns=False)
    store.check_budget_columns(f, dict(tables=[dict(ready=True, value_columns=columns(('approved', 2022)))]))
    assert f['fiscal_year'] == 2022 and not f['expected_columns']
    assert f['issues'] == 'units-unknown,missing-executed,missing-approved'

    update = dict(doc_type='budget_update', fiscal_year=2022, issues=None)
    store.check_budget_columns(update, dict(tables=[dict(ready=True, value_columns=columns(('revised', 2022)))]))
    assert update['issues'] is None


def test_budget_lines_take_a_table_published_twice_once(monkeypatch):
    import budget_files_scraper as scraper
    files = [dict(authority_code='4000', entity_id='500', name='חיפה', fiscal_year=2025, doc_type='budget',
                  format='xlsx', ready_to_process=True, file_url='https://x/%s.xlsx' % n, stored_url='s3://%s' % n)
             for n in ('details', 'workbook')]
    value_columns = json.dumps(columns(('executed', 2023), ('approved', 2024), ('proposed', 2025)))
    tables = [dict(file_url=f['file_url'], table_index=i, ready=True, units='thousands', income_sign='positive',
                   expense_sign='positive', duplicate_columns=[], value_columns=value_columns)
              for f in files for i in (0, 1)]
    details = [dict(table_index='0', sheet='פירוט', row='7', code='1611000110', title='שכר', column=str(c),
                    kind='approved', year=str(2023 + c), header='', value=str(100 + c)) for c in range(3)]
    summary = [dict(row, table_index='1', sheet='ריכוז', code='1611000999', value='5') for row in details]
    monkeypatch.setattr(store, 'read_rows',
                        lambda f: details if 'details' in f['file_url'] else details + summary)
    lines = list(scraper.budget_lines(files, tables))
    assert [(l['file_url'][-13:], l['column_kind'], l['column_year']) for l in lines] == [
        ('/details.xlsx', 'executed', 2023), ('/details.xlsx', 'approved', 2024), ('/details.xlsx', 'proposed', 2025),
        ('workbook.xlsx', 'executed', 2023), ('workbook.xlsx', 'approved', 2024), ('workbook.xlsx', 'proposed', 2025)]
    assert [l['code'] for l in lines[3:]] == ['1611000999'] * 3


def test_the_scraper_reruns_on_a_new_analyzer():
    import budget_files_scraper as scraper
    assert scraper.ANALYZER_VERSION == a.ANALYZER_VERSION
