
import json

import dataflows as DF

def clean_lines(text: str):
    prefix = ''
    while text and text[0] in [' ', '\t', '\n']:
        prefix += text[0]
        text = text[1:]
    if prefix and prefix.startswith('\n'):
        text = text.replace(prefix, '\n')
    return text.strip()

def filtered_budget_code(code):
    if not code:
        return None
    if code.startswith('0000'):
        return None
    if code == '00':
        return None
    if code.startswith('C'):
        return None
    code = code[2:]
    ret = ''
    while code:
        ret += code[:2] + '.'
        code = code[2:]
    return ret[:-1]

def get_filter(func, field_name):
    def filter_func(row):
        value = row.get(field_name)
        return func(value)
    return filter_func

def debug_source(source, debug):
    if debug:
        return source.replace('/var/datapackages/', 'https://next.obudget.org/datapackages/', )
    return source

PARAMETERS = dict(
    budget_items_data=dict(
        source='/var/datapackages/budget/national/processed/with-extras',
        # source='https://next.obudget.org/datapackages/budget/national/processed/with-extras',
        details='''
            סעיפי התקציב מספר תקציב המדינה.
            המידע הוא לכלל שנות התקציב מאז 1997 ועד השנה הנוכחית (2024).
        ''',
        fields=[
            dict(
                name='code',
                description='''
                    קוד הסעיף התקציבי.
                    קבוצות של שתי ספרות מופרדות על ידי נקודה.
                    ככל שהקוד יותר ארוך, כך הסעיף יותר מפורט, למשל:
                    - 20: תקציב משרד החינוך
                        סעיפים מסוג זה מכילים תקציבים של כלל המשרדים
                        נקראים גם סעיפי 2 ספרות
                    - 20.67: פעולות משלימות
                        סעיפים ברמה זו מכילים תקציבים של תחומי פעילות
                        נקראים גם סעיפי 4 ספרות
                    - 20.67.01: פעילויות ופרוייקטים
                        סעיפים ברמה זו נקראים גם ״תכניות תקציביות״
                        נקראים גם סעיפי 6 ספרות
                    - 20.67.01.42: תמיכה בתנועות נוער
                        סעיפים ברמה זו נקראים גם ״תקנות תקציביות״
                        נקראים גם סעיפי 8 ספרות
                ''',
                sample_values=['20', '20.67', '20.67.01', '20.67.01.42'],
                transform=filtered_budget_code,
                filter=lambda x: x is not None,
            ),
            dict(
                name='title',
                description='''
                    כותרת הסעיף התקציבי.
                    כותרת זו מתארת את תכנית התקציב של הסעיף.
                    הכותרת בדרך כלל קצרה ולא מאוד תיאורית.
                ''',
                sample_values=['תמיכה בתנועות נוער', 'מלוות סחירים', 'קורונה - אבטלה']
            ),
            dict(
                name='year',
                description='''
                    שנת התקציב.
                    שנת התקציב ממנו נלקח הסעיף.
                ''',
                sample_values=[2017, 2023, 2024]
            ),
            dict(
                name='amount_allocated',
                description='''
                    סכום התקציב המתוכנן (נקרא גם תקציב מקורי).
                    סכום התקציב של הסעיף בשקלים.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: row.get('net_allocated')
            ),
            dict(
                name='amount_revised',
                description='''
                    סכום התקציב המתוקן, לאחר שבוצעו עליו שינויים במהלך השנה.
                    נקרא גם תקציב מאושר״ או ״תקציב על שינוייו״.
                    סכום התקציב של הסעיף לאחר שינויים בשקלים.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: row.get('net_revised', row.get('net_allocated'))
            ),
            dict(
                name='amount_used',
                description='''
                    סכום התקציב שנוצל במהלך השנה.
                    נקרא גם ״תקציב מבוצע״ או ״ביצוע תקציב״.
                    סכום התקציב של הסעיף שנוצל בשקלים.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: row.get('net_executed')
            ),
            dict(
                name='level',
                description='''
                    רמת הסעיף התקציבי.
                    1 מייצג סעיפים ברמה הגבוהה ביותר (כמו משרדים ראשיים).
                    4 מייצג סעיפים ברמה הנמוכה ביותר (תקנות תקציביות).
                ''',
                sample_values=[1, 2, 3, 4],
                type='integer',
                default=lambda row: len(row['code'].split('.'))
            ),
            dict(
                name='functional_class_primary',
                description='',
                sample_values=[1, 2, 3, 4],
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['func_cls_json'][0])[1],
            ),
            dict(
                name='functional_class_main',
                description='',
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['func_cls_json'][0])[3],
            ),
            dict(
                name='economic_class_primary',
                description='',
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['econ_cls_json'][0])[1],
            ),
            dict(
                name='economic_class_main',
                description='',
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['econ_cls_json'][0])[3],
            ),
        ]
    )
)

def get_flow(table, params, debug=False):
    steps = []
    source = debug_source(params['source'], debug)
    details = clean_lines(params['details'])
    fields = params['fields']
    steps.append(DF.load(f'{source}/datapackage.json', limit_rows=10000 if debug else None))
    steps.append(DF.update_resource(-1, details=details, name=table))
    
    field_names = []
    for field in fields:
        field['description'] = clean_lines(field['description'])
        field_name = field.pop('name')
        field_names.append(field_name)
        if 'default' in field:
            default = field.pop('default')
            datatype = field.get('type')
            steps.append(DF.add_field(field_name, type=datatype, default=default))
        elif 'transform' in field:
            transform = field.pop('transform')
            steps.append(DF.set_type(field_name, transform=transform))
        if 'filter' in field:
            filter_func = field.pop('filter')
            steps.append(DF.filter_rows(get_filter(filter_func, field_name)))
        steps.append(DF.set_type(field_name, details=field))

    steps.append(DF.select_fields(field_names))
    if not debug:
        steps.append(DF.dump_to_path(f'/var/datapackages/simpledb/{table}'))
        steps.append(DF.dump_to_sql({table: {'resource-name': table}}))
    else:
        steps.append(DF.printer())
    return DF.Flow(*steps)

def flow(parameters, *_):
    for table, params in PARAMETERS.items():
        get_flow(table, params).process()
    return DF.Flow(
        [dict(done=True)],
        DF.update_resource(-1, **{'dpp:streaming': True}),    
    )


if __name__ == '__main__':
    params = PARAMETERS['budget_items_data']
    DF.Flow(
        get_flow('budget_items_data', params, debug=True),
    ).process()