
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

def filtered_budget_code_income(code):
    if not code:
        return None
    if not code.startswith('0000'):
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
        description='''
            סעיפי התקציב מספר תקציב המדינה.
            המידע הוא לכלל שנות התקציב מאז 1997 ועד השנה הנוכחית (2024).
            חיפוש טקסט חופשי - לפי שמות הסעיפים *בלבד*!
            השתמש בשדה code בשליפות ב-db ולא בשדה title.
        ''',
        fields=[
            dict(
                name='code',
                description='''
                    קוד הסעיף התקציבי.
                    לסעיפים תקציביים מבנה היררכי.
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
                    כותרת זו מתארת את מהות ההוצאה התקציבית.
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
                description='''
                    הקטגוריה הנושאית הראשית של תקנה תקציבית (functional classification).
                    קטגוריות אלו מגדירות את סוג הפעילות של התקנה התקציבית, לפי המטרה שעבורה היא משמשת.
                    בתוך כל קטגוריה יש קטגוריות משנה, המצויות בשדה functional_class_secondary.
                ''',
                possible_values=[
                    'שירותים חברתיים',
                    'החזרי חוב',
                    'בטחון וסדר ציבורי',
                    'תשתיות',
                    'משרדי מטה',
                    'הוצאות אחרות',
                    'ענפי משק'
                ],
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['func_cls_json'][0])[1],
            ),
            dict(
                name='functional_class_secondary',
                description='''
                    הקטגוריה הנושאית המשנית של תקנה תקציבית.
                ''',
                type='string',
                sample_values=[
                    'קרן',
                    'בטחון',
                    'חינוך',
                    'בריאות',
                    'ריבית',
                    'העברות לביטוח הלאומי',
                    'תחבורה',
                    'רשות מקרקעי ישראל',
                    'בטחון פנים',
                ],
                default=lambda row: None if row['level'] != 4 else json.loads(row['func_cls_json'][0])[3],
            ),
            dict(
                name='economic_class_primary',
                description='''
                    הקטגוריה הכלכלית הראשית של תקנה תקציבית (economic classification).
                    קטגוריות אלו מגדירות את אופן השימוש בתקציב בתקנה התקציבית.
                    בתוך כל קטגוריה יש קטגוריות משנה, המצויות בשדה economic_class_secondary.
                ''',
                sample_values=[
                    'העברות',
                    'החזר חוב - קרן',
                    'שכר',
                    'קניות',
                    'החזר חוב - ריבית',
                    'השקעה',
                    'הכנסות מיועדות',
                    'מתן אשראי',
                    'הוצאות הון',
                    'רזרבות',
                    'חשבונות מעבר',
                    'העברות פנים תקציביות',                    
                ],
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['econ_cls_json'][0])[1],
            ),
            dict(
                name='economic_class_secondary',
                description='''
                    הקטגוריה הכלכלית המשנית של תקנה תקציבית.
                ''',
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['econ_cls_json'][0])[3],
            ),
        ],
        search=dict(
            index='budget',
            field_map={
                'code': 'nice-code',
                'title': 'title',
                'year': 'year',
                'amount_allocated': 'net_allocated',
                'amount_revised': 'net_revised',
                'amount_used': 'net_executed',
            },
            filters={
                'func_cls_title_1__not': 'הכנסות',
                'depth__gt': 0
            }
        )
    ),
    income_items_data=dict(
        source='/var/datapackages/budget/national/processed/with-extras',
        # source='https://next.obudget.org/datapackages/budget/national/processed/with-extras',
        description='''
            סעיפי הכנסות המדינה מתוך ספר תקציב המדינה.
            המידע הוא לכלל שנות התקציב מאז 1997 ועד השנה הנוכחית (2024).
            חיפוש טקסט חופשי - לפי שמות הסעיפים *בלבד*!
            השתמש בשדה code בשליפות ב-db ולא בשדה title.
        ''',
        fields=[
            dict(
                name='code',
                description='''
                    קוד סעיף ההכנסה.
                    לסעיפים ההכנסה מבנה היררכי.
                    קבוצות של שתי ספרות מופרדות על ידי נקודה.
                    ככל שהקוד יותר ארוך, כך הסעיף יותר מפורט, למשל:
                    - 00: כלל ההכנסות
                    - 00.01: מס הכנסה
                    - 00.01.03: מס הכנסה סקטור עסקי
                    - 00.01.03.01: מס הכנסה חברות
                ''',
                sample_values=['00', '00.01', '00.01.03', '00.01.03.01'],
                transform=filtered_budget_code_income,
                filter=lambda x: x is not None,
            ),
            dict(
                name='title',
                description='''
                    כותרת סעיף ההכנסה.
                    כותרת זו מתארת את מהות ההכנסה.
                    הכותרת בדרך כלל קצרה ולא מאוד תיאורית.
                ''',
                sample_values=['מס רכישה', 'דיוידנדים מחברות ממשלתי', 'גביית ריבית']
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
                    סכום ההכנסה המתוכננת בשקלים.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: row.get('net_allocated')
            ),
            dict(
                name='amount_revised',
                description='''
                    סכום ההכנסה המתוקנת בשקלים, לאחר שינויים במהלך השנה.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: row.get('net_revised', row.get('net_allocated'))
            ),
            dict(
                name='amount_used',
                description='''
                    ההכנסה בפועל בשקלים.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: row.get('net_executed')
            ),
            dict(
                name='level',
                description='''
                    רמת סעיף ההכנסה.
                    1 מייצג את סעיף כלל ההכנסות (00)
                    4 מייצג סעיפים ברמה המפורטת ביותר
                ''',
                sample_values=[1, 2, 3, 4],
                type='integer',
                default=lambda row: len(row['code'].split('.'))
            ),
            dict(
                name='functional_class',
                description='''
                    הקטגוריה הנושאית הראשית של תקנה תקציבית (functional classification).
                    קטגוריות אלו מגדירות את סוג הפעילות של התקנה התקציבית, לפי המטרה שעבורה היא משמשת.
                    בתוך כל קטגוריה יש קטגוריות משנה, המצויות בשדה functional_class_secondary.
                ''',
                possible_values=[
                    'אגרות',
                    'הכנסות אחרות',
                    'הכנסות למימון גירעון',
                    'מסים ישירים',
                    'מסים עקיפים',
                ],
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['func_cls_json'][0])[3],
            ),
            dict(
                name='economic_class_primary',
                description='''
                    הקטגוריה הכלכלית הראשית של ההכנסה (economic classification).
                    בתוך כל קטגוריה יש קטגוריות משנה, המצויות בשדה economic_class_secondary.
                ''',
                sample_values=[
                    'הכנסות המדינה - מענקים',
                    'הכנסות המדינה - מילוות',
                    'הכנסות המדינה - הכנסות',
                    'הכנסות המדינה - בנק ישראל',
                ],
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['econ_cls_json'][0])[1],
            ),
            dict(
                name='economic_class_secondary',
                description='''
                    הקטגוריה הכלכלית המשנית של ההכנסה.
                ''',
                sample_values=[
                    'בנק ישראל',
                    'הכנסות אחרות',
                    'הכנסות הון',
                    'הכנסות ממיסים',
                    'מילוות זרים',
                    'מילוות מקומיים',
                    'מענקים אזרחיים',
                    'מענקים בטחוניים',
                ],
                type='string',
                default=lambda row: None if row['level'] != 4 else json.loads(row['econ_cls_json'][0])[3],
            ),
        ],
        search=dict(
            index='budget',
            field_map={
                'code': 'nice-code',
                'title': 'title',
                'year': 'year',
                'amount_allocated': 'net_allocated',
                'amount_revised': 'net_revised',
                'amount_used': 'net_executed',
            },
            filters={
                'func_cls_title_1': 'הכנסות',
                'depth__gt': 1
            }
        )
    ),
    supports_data=dict(
        source='/var/datapackages/supports/with-entities',
        description='''
            נתוני תמיכות תקציביות בחברות ועמותות.
            תמיכות תקציביות הן לא התקשרויות רכש, אלא ניתנות תחת קריטריונים מוגדרים היטב (״מבחני תמיכה״)
            המידע הוא לכלל שנות התקציב מאז 2004 ועד השנה הנוכחית (2024).
            חיפוש טקסט חופשי - לפי שם המשרדו, מטרת התמיכה *בלבד*!
            השתמש בשדות budget_code ו entity_id בשליפות ב-db ולא בשדה entity_name או support_title.
        ''',
        fields=[
            dict(
                name='budget_code',
                description='''
                    קוד הסעיף התקציבי ממנו משולמת התמיכה.
                    קבוצות של שתי ספרות מופרדות על ידי נקודות.
                ''',
                sample_values=['26.13.01.07', '20.67.01.42', '19.42.02.26'],
                transform=filtered_budget_code,
                filter=lambda x: x is not None,
            ),
            dict(
                name='purpose',
                description='''
                    מטרת התמיכה התקציבית.
                    שדה זה בדרך כלל קצר ולא מאוד תיאורי.
                ''',
                sample_values=['פסטיבלים לתרבות ואומנות - תמיכה', 'סל לתרבות עירונית', 'חוק הפיקדון - תמיכהברשויות'],
                type='string',
                default=lambda row: row.get('support_title')
            ),
            dict(
                name='supporting_ministry',
                description='''
                    שם המשרד שמבצע את התמיכה.
                    חשוב לשים לב ששמות המשרדים משתנים ועשויים להפויע בצורות שונות בתמיכות שונות.
                ''',
                sample_values=['משרד התרבות והספורט', 'משרד הפנים', 'משרד החינוך'],
            ),
            dict(
                name='request_type',
                description='''
                    הסיווג הכלכלי/חוקי של התמיכה
                ''',
                possible_values=[
	                'בקשת תמיכה אחרים',
                    'תמיכה לרשות מקומית',
                    'מענקים על פי נוסחה',
                    'א3',
                    'הקצבות',
                    'תמיכה דרך מוסד',
                    'תמיכה כלכלית',
                    'בקשת תמיכה לפרט',
                ],
            ),
            dict(
                name='value_kind',
                description='''
                    האם שורה זו מציינת תקציב מאושר או תשלום בפועל.
                ''',
                possible_values=['approval', 'payment'],
                type='string',
                default=lambda row: 'payment' if row.get('year_paid') else 'approval',
            ),
            dict(
                name='year',
                description='''
                    שנת אישור התמיכה (או ביצוע התשלום, לפי value_kind).
                ''',
                sample_values=[2017, 2023, 2024]
            ),
            dict(
                name='amount',
                description='''
                    הסכום שאושר או ששולם בפועל במסגרת התמיכה.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: row.get('amount_approved') if row['value_kind'] == 'approval' else row.get('amount_total'),
                filter=lambda x: x is not None and x > 0
            ),
            dict(
                name='recipient_entity_name',
                description='''
                    השם הרשמי של מקבל התמיכה (יכול להיות השם של חברה פרטית, עמותה, רשות מקומית וכד׳)
                ''',
                sample_values=['מ. א. מנשה', 'שירותי בריאות כללית', 'עירית אשקלון', 'מעון שירת הרך בע"מ'],
                type='string',
                default=lambda row: row.get('entity_name', row.get('recipient')),
                fiilter=lambda x: x is not None
            ),
            dict(
                name='recipient_entity_id',
                description='''
                    מספר התאגיד של מקבל התמיכה.
                    יכול להיות ריק אם מקבל התמיכה הוא אדם פרטי
                ''',
                sample_values=['570053512', '589906114', '513705699'],
                type='string',
                default=lambda row: row.get('entity_id')
            ),
            dict(
                name='recipient_entity_kind',
                description='''
                    סוג הארגון שמקבל את התמיכה.
                ''',
                most_common_values=[
                    'municipality',
                    'company',
                    'association',
                    'ottoman-association',
                    'provident_fund',
                    'cooperative',
                    'private_person',
                ],
                type='string',
                default=lambda row: row.get('entity_kind') or 'private_person'
            ),
        ],
        search=dict(
            index='supports',
            field_map={
                'budget_code': 'nice-code',
                'purpose': 'support_title',
                'supporting_ministry': 'supporting_ministry',
                'request_type': 'request_type',
                'receiver_entity_name': ['entity_name', 'recipient'],
                'recipient_entity_id': 'entity_id',
                'recipient_entity_kind': 'entity_kind',
                '__approval_year': 'year_requested',
                '__last_payment_year': 'last_payment_year',
                '__total_paid_amount': 'amount_total',
            },
            filters={}
        )
    ),
)

def get_flow(table, params, debug=False):
    steps = []
    source = debug_source(params['source'], debug)
    description = clean_lines(params['description'])
    fields = params['fields']
    search = params.get('search')
    steps.append(DF.load(f'{source}/datapackage.json', limit_rows=10000 if debug else None))
    steps.append(DF.update_resource(-1, description=description, name=table, search=search))
    
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