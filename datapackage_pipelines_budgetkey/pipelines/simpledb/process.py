import csv

csv.field_size_limit(2*1024*1024)

import json

import dataflows as DF

def lowest_level(row, max_level=4):
    return row['level'] == max_level or (row['level'] == max_level-1 and row['is_proposal'])

def clean_lines(text: str):
    prefix = ''
    while text and text[0] in [' ', '\t', '\n']:
        prefix += text[0]
        text = text[1:]
    if prefix and prefix.startswith('\n'):
        text = text.replace(prefix, '\n')
    return text.strip()

def clean_budget_code(code):
    if not code:
        return None
    code = code[2:]
    ret = ''
    while code:
        ret += code[:2] + '.'
        code = code[2:]
    return ret[:-1]

def filtered_budget_code(code):
    if not code:
        return None
    if code.startswith('0000'):
        return None
    if code == '00':
        return 'TOTAL'
    if code.startswith('C'):
        return None
    return clean_budget_code(code)

def filtered_budget_code_income(code):
    if not code:
        return None
    if not code.startswith('0000'):
        return None
    if code == '0000':
        return 'TOTAL'
    code = code[2:]
    return clean_budget_code(code)

def get_filter(func, field_name):
    def filter_func(row):
        value = row.get(field_name)
        return func(value)
    return filter_func

def debug_source(source, debug):
    if debug:
        return source.replace('/var/datapackages/', 'https://next.obudget.org/datapackages/', )
    return source

def item_url(kind, fields):
    return dict(
        name='item_url',
        description='''
            קישור לעמוד הפריט באתר מפתח התקציב.
        ''',
        type='string',
        default=lambda row: f'https://next.obudget.org/i/{kind}/' + '/'.join([str(row[f]) for f in fields])
    )

def convert_request_summary(row):
    ret = dict()
    summary = row.get('summary') or dict()
    for f in ['to', 'from']:
        if f in summary:
            items = [
                dict(
                    code=code,
                    office=office,
                    amount=amount
                )
                for code, amount, office in summary[f]
            ]
            ret[f] = items
    return ret


PARAMETERS = dict(
    budget_items_data=dict(
        source='/var/datapackages/budget/national/processed/with-extras',
        # source='https://next.obudget.org/datapackages/budget/national/processed/with-extras',
        description='''
            סעיפי התקציב מספר תקציב המדינה.
            המידע הוא לכלל שנות התקציב מאז 1997 ועד השנה הנוכחית (2024).
            בנוסף זמינה גם הצעת התקציב לשנת 2025.

            אופן ביצוע שאילתות database:
            - חיפוש סעיף תקציבי רק על פי קוד סעיף תקציבי.
            - אם לא יודעים את קוד הסעיף, חובה לבצע חיפוש טקסט חופשי מקדים!
            - השתמש בשדה code בשליפות ב-db ולא בשדה title.
            - מידע על כלל תקציב המדינה יש לקבל על ידי שליפה של code="TOTAL".
            - אסור לבצע שאילתה שמסכמת ערכי תקציב עם 
              `WHERE code LIKE 'XX%'`
              כי זה יכלול את הסעיף יחד עם הבנים שלו וזו שגיאה.
            - לסינון נושאים כלליים השתמש בשדה functional_class_detailed.
        ''',
        fields=[
            item_url('budget', ['code', 'year']),
            dict(
                name='code',
                description='''
                    קוד הסעיף התקציבי.
                    לסעיפים תקציביים מבנה היררכי.
                    קבוצות של שתי ספרות מופרדות על ידי נקודה.
                    ככל שהקוד יותר ארוך, כך הסעיף יותר מפורט, למשל:
                    - 20: תקציב משרד החינוך
                        סעיפים מסוג זה מכילים תקציבים של כלל המשרדים
                        נקראים גם סעיפי 2 ספרות או ״סעיפים ראשיים״
                    - 20.67: פעולות משלימות (בתוך משרד החינוך)
                        סעיפים ברמה זו מכילים תקציבים של תחומי פעילות
                        נקראים גם סעיפי 4 ספרות
                    - 20.67.01: פעילויות ופרוייקטים (בתוך פעולות משלימות)
                        סעיפים ברמה זו נקראים גם ״תכניות תקציביות״
                        נקראים גם סעיפי 6 ספרות
                    - 20.67.01.42: תמיכה בתנועות נוער (בתוך פעילויות ופרוייקטים)
                        סעיפים ברמה זו נקראים גם ״תקנות תקציביות״
                        נקראים גם סעיפי 8 ספרות

                    הערך המיוחד "TOTAL" מייצג את כלל תקציב המדינה.

                    מומלץ תמיד לוודא את הקוד הנכון של סעיף תקציבי בעזרת חיפוש טקסט חופשי.

                    הערכים של כל סעיף כבר כוללים את כל הסעיפים הנמצאים ברמות מתחתיו, לכן:
                    *אסור* לבצע שאילתה שמסכמת ערכי תקציב עם WHERE code LIKE 'XX%' כי זה יכלול את הסעיף יחד עם הבנים שלו וזו שגיאה!
                ''',
                sample_values=['15', '04.51', '23', '20.67.01.42', '84.05.01', '79.55.01', 'TOTAL'],
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
                    הערך 0 מייצג את כלל תקציב המדינה.
                ''',
                sample_values=[0, 1, 2, 3, 4],
                type='integer',
                default=lambda row: 0 if row['code'] == 'TOTAL' else len(row['code'].split('.'))
            ),
            dict(
                name='functional_class_top_level',
                description='''
                    הקטגוריה הנושאית הכללית של תקנה תקציבית (functional classification).
                    קטגוריות אלו מגדירות את סוג הפעילות הכללית של התקנה התקציבית, לפי המטרה שעבורה היא משמשת.
                    בתוך כל קטגוריה יש קטגוריות משנה, המצויות בשדה functional_class_detailed.
                    מוגדר רק עבור שורות בהן level=4.
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
                default=lambda row: None if not lowest_level(row) else json.loads(row['func_cls_json'][0])[1],
            ),
            dict(
                name='functional_class_detailed',
                description='''
                    הקטגוריה הנושאית (functional classification) המפורטת של תקנה תקציבית.
                    קטגוריות אלו מגדירות את סוג הפעילות המפורטת של התקנה התקציבית, לפי המטרה שעבורה היא משמשת.
                    ניתן לסכום את כל הסעיפים לפי השדה הזה כדי לדעת כמה כסף הושקע בנושא כלשהו.
                    מוגדר רק עבור שורות בהן level=4.
                ''',
                type='string',
                sample_values=[
                    'בטחון',
                    'חינוך',
                    'בריאות',
                    'העברות לביטוח הלאומי',
                    'ריבית',
                    'רשות מקרקעי ישראל',
                    'תחבורה',
                    'בטחון פנים',
                    'גמלאות',
                    'בטחון-אחר',
                    'קרן - ביטוח לאומי',
                    'רווחה',
                    'השכלה גבוהה',
                    'הוצאות שונות',
                    'פנים ושלטון מקומי',
                    'בינוי ושיכון',
                    'אוצר',
                    'משפטים',
                    'כלכלה ותעשיה',
                    'ראש הממשלה',
                    'תעסוקה',
                    'תיירות',
                    'מדע, תרבות וספורט',
                    'חקלאות',
                    'חוץ',
                    'קליטת עליה',
                    'משרדים נוספים',
                    'שירותי דת',
                    'משק המים',
                    'אנרגיה',
                    'הגנת הסביבה',
                    'תקשורת',
                    'רזרבה',
                ],
                default=lambda row: None if not lowest_level(row) else json.loads(row['func_cls_json'][0])[3],
            ),
            dict(
                name='economic_class_primary',
                description='''
                    הקטגוריה הכלכלית הראשית של תקנה תקציבית (economic classification).
                    קטגוריות אלו מגדירות את אופן השימוש בתקציב בתקנה התקציבית.
                    בתוך כל קטגוריה יש קטגוריות משנה, המצויות בשדה economic_class_secondary.
                    מוגדר רק עבור שורות בהן level=4.
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
                default=lambda row: None if not lowest_level(row) else json.loads(row['econ_cls_json'][0])[1],
            ),
            dict(
                name='economic_class_secondary',
                description='''
                    הקטגוריה הכלכלית המשנית של תקנה תקציבית.
                    מוגדר רק עבור שורות בהן level=4.
                ''',
                type='string',
                default=lambda row: None if not lowest_level(row) else json.loads(row['econ_cls_json'][0])[3],
            ),
        ],
        search=dict(
            index='budget',
            field_map={
                'code': 'nice-code',
                'title': 'title',
                'year-range': 'year-range',
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
            בנוסף זמינה גם הערכת ההכנסות לתקציב לשנת 2025.

            חיפוש טקסט חופשי - לפי שמות הסעיפים *בלבד*!
            השתמש בשדה code בשליפות ב-db ולא בשדה title.
        ''',
        fields=[
            item_url('budget', ['code', 'year']),
            dict(
                name='code',
                description='''
                    קוד סעיף ההכנסה.
                    לסעיפים ההכנסה מבנה היררכי.
                    קבוצות של שתי ספרות מופרדות על ידי נקודה.
                    ככל שהקוד יותר ארוך, כך הסעיף יותר מפורט, למשל:
                    - TOTAL: כלל ההכנסות
                    - 01: מס הכנסה
                    - 01.03: מס הכנסה סקטור עסקי
                    - 01.03.01: מס הכנסה חברות
                ''',
                sample_values=['TOTAL', '01', '01.03', '01.03.01'],
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
                    הקטגוריה הנושאית של תקנת ההכנסה תקציבית (functional classification).
                    קטגוריות אלו מגדירות את סוג ההכנסה של התקנה התקציבית.''',
                possible_values=[
                    'אגרות',
                    'הכנסות אחרות',
                    'הכנסות למימון גירעון',
                    'מסים ישירים',
                    'מסים עקיפים',
                ],
                type='string',
                default=lambda row: None if not lowest_level(row, 3) else json.loads(row['func_cls_json'][0])[3],
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
                default=lambda row: None if not lowest_level(row, 3) else json.loads(row['econ_cls_json'][0])[1],
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
                default=lambda row: None if not lowest_level(row, 3) else json.loads(row['econ_cls_json'][0])[3],
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
    entities_data=dict(
        source='/var/datapackages/entities/scored',
        description='''
            נתונים על תאגידים בישראל -
            חברות, עמותות, רשויות מקומיות ועוד.
        ''',
        fields=[
            item_url('entities', ['id']),
            dict(
                name='entity_id',
                description='''
                    מספר מזהה ייחודי של התאגיד.
                    אפשר להשתמש בו אח״כ בשאילתות בשדות *entity_id (בלבד).
                ''',
                sample_values=['570053512', '589906114', '513705699'],
                type='string',
                default=lambda row: row.get('id'),
                filter=lambda x: x is not None,
            ),
            dict(
                name='entity_name',
                description='''
                    שם התאגיד הרשמי.
                ''',
                sample_values=['מ. א. מנשה', 'שירותי בריאות כללית', 'עירית אשקלון', 'מעון שירת הרך בע"מ'],
                type='string',
                default=lambda row: row.get('name'),
                filter=lambda x: x is not None
            ),
            dict(
                name='entity_name__en',
                description='''
                    שם התאגיד באנגלית, אם קיים.
                ''',
                type='string',
                default=lambda row: row.get('name_en'),
            ),
            dict(
                name='entity_kind',
                description='''
                    סוג הארגון שמקבל את התמיכה (מזהה הסוג)
                ''',
                sample_values=[
                    'municipality',
                    'company',
                    'association',
                    'ottoman-association',
                    'provident_fund',
                    'cooperative',
                    'law_mandated_organization',
                ],
                type='string',
                default=lambda row: (row.get('kind'))
            ),
            dict(
                name='entity_kind__he',
                description='''
                    סוג הארגון שמקבל את התמיכה (שם בעברית)
                ''',
                sample_values=[
                    'רשות מקומית',
                    'חברה פרטית',
                    'עמותה',
                    'אגודה עותמנית',
                    'קופת גמל',
                    'אגודה שיתופית',
                    'תאגיד סטטוטורי',
                ],
                type='string',
                default=lambda row: (row.get('kind_he'))
            ),

            dict(
                name='received_amount',
                description='''
                    כמה כספים קיבל הארגון מהמדינה בשלוש השנים האחרונות, בין אם כתמיכה תקציבית או בהתקשרות רכש.
                ''',
                sample_values=[0, 1000, 12998431556.76],
            ),
        ],
        search=dict(
            index='entities',
            field_map={
                'entity_id': 'id',
                'entity_name': 'name',
                'entity_kind': 'kind',
                'entity_kind__he': 'kind_he',
                'received_amount': 'received_amount',
                'entity_name__en': 'name_en',
            },
            filters={}
        )
    ),
    contracts_data=dict(
        source='/var/datapackages/procurement/spending/latest-contract-spending',
        description='''
            נתוני התקשרויות רכש עם חברות, עמותות וספקים אחרים.
            התקשרויות רכש משמשות לקניית מוצרים ושירותים.
            המידע הוא לכלל שנות התקציב מאז 2016 ועד השנה הנוכחית (2024).
            
            חיפוש טקסט חופשי - לפי שם המשרד, ומטרת ההתקשרות *בלבד*!
            בשאילתות database:
                - השתמש בשדות budget_code ו supplier_entity_id בשביל לסנן
                - אל תסנן לפי  supplier_entity_name או purpose.
                - לפני סינון לפי שדה purchasing_ministry או purchasing_method, בדוק את הערכים הזמינים (distinct query) כדי לבחור בצורה נכונה.

            בשביל לדעת אילו התקשרויות או ספקים היו פעילים בשנה מסוימת, חובה להשתמש בשדות start_year ו end_year
        ''',
        fields=[
            item_url('contract-spending', ['publisher_key', 'order_id', 'budget_code']),
            dict(
                name='budget_code',
                description='''
                    קוד הסעיף התקציבי ממנו בוצע הרכש.
                    קבוצות של שתי ספרות מופרדות על ידי נקודות.
                ''',
                sample_values=['26.13.01.07', '20.67.01.42', '19.42.02.26'],
                transform=filtered_budget_code,
                filter=lambda x: x is not None,
            ),
            dict(
                name='purpose',
                description='''
                    מטרת התקשרות הרכש.
                    שדה זה יכול להיות די קצר ולא מאוד תיאורי.
                ''',
                sample_values=['פסטיבלים לתרבות ואומנות - תמיכה', 'סל לתרבות עירונית', 'חוק הפיקדון - תמיכהברשויות'],
                type='string',
            ),
            dict(
                name='purchasing_ministry',
                description='''
                    שם המשרד שביצע את הרכש.
                    חשוב לשים לב ששמות המשרדים משתנים ועשויים להפויע בצורות שונות ההתקשרויות שונות.
                ''',
                most_common_values=[
                    'משרד התחבורה והבטיחות בדרכים',
                    'משרד הבינוי והשיכון',
                    'רשות מקרקעי ישראל',
                    'משרד הבריאות',
                    'משרד החינוך',
                    'משרד האוצר',
                    'המשרד להגנת הסביבה',
                    'המשרד לשוויון חברתי',
                    'משרד הכלכלה והתעשייה',
                    'משרד המשפטים',
                    'משרד הרווחה והביטחון החברתי',
                    'משרד ראש הממשלה',
                    'משרד הפנים',
                ],
                type='string',
                default=lambda row: row.get('publisher_name')
            ),
            dict(
                name='purchasing_method',
                description='''
                    אופן הרכישה - האם היא נעשתה במכרז פתוח, בפטור ממכרז או בכל דרך אחרת.
                ''',
                sample_values=[
                    'פטור ממכרז',
                    'תקנה 14ב - מכרז מרכזי',
                    'תקנה 1ב - מכרז פומבי רגיל',
                    'התקשרות עם גופים ממשלתיים',
                    'מינוי חברים בועדות',
                    'תשלומי חשמל/מים/ארנונה/כבישי אגרה',
                    'תקנה 5א(ב)(1) - התקשרות עם מתכננים',
                    'מענקים לרשויות מקומיות',
                    'אחר',
                ],
                type='string',
                default=lambda row: (row.get('purchase_method') or [None])[0]
            ),
            dict(
                name='order_date',
                description='''
                    תאריך ביצוע ההזמנה המקורית
                ''',
                sample_values=['2021-01-01', '2023-12-31', '2024-02-29'],
                type='date',                
            ),
            dict(
                name='start_year',
                description='''
                    שנת תחילת ההתקשרות
                ''',
                sample_values=[2017, 2020, 2024],
                type='integer',
                default=lambda row: row.get('min_year')
            ),
            dict(
                name='end_year',
                description='''
                    שנת סוף ההתקשרות (או שנת הפעילות האחרונה של ההתקשרות).
                ''',
                sample_values=[2017, 2020, 2024],
                type='integer',
                default=lambda row: row.get('max_year')
            ),
            dict(
                name='end_date',
                description='''
                    תאריך סיום ההתקשרות.
                ''',
                sample_values=['2021-01-01', '2023-12-31', '2024-02-29'],
                type='date',                
            ),
            dict(
                name='is_active',
                description='''
                    האם ההתקשרות פעילה כיום.
                ''',
                sample_values=[True, False],
                type='boolean',
                default=lambda row: row.get('contract_is_active')
            ),
            dict(
                name='volume',
                description='''
                    הסכום הכולל שאושר עבור ההתקשרות.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                filter=lambda x: x is not None and x > 0
            ),
            dict(
                name='executed',
                description='''
                    הסכום הכולל ששולם עד כה עבור ההתקשרות.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
            ),
            dict(
                name='volume_per_year',
                description='''
                    היקף ההתקשרות ההמוצע לשנה.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: (row.get('volume') or 0) / (row.get('max_year') - row.get('min_year') + 1) if row.get('max_year') and row.get('min_year') else None
            ),
            dict(
                name='executed_per_year',
                description='''
                    הסכום הממוצע לשנה ששולם בהתקשרות.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: (row.get('executed') or 0) / (row.get('max_year') - row.get('min_year') + 1) if row.get('max_year') and row.get('min_year') else None
            ),
            dict(
                name='supplier_entity_name',
                description='''
                    השם הרשמי של הספק (יכול להיות השם של חברה פרטית, עמותה, רשות מקומית וכד׳)
                ''',
                sample_values=['מ. א. מנשה', 'שירותי בריאות כללית', 'עירית אשקלון', 'מעון שירת הרך בע"מ'],
                type='string',
                default=lambda row: row.get('entity_name', (row.get('supplier') or [None])[0]),
                filter=lambda x: x is not None
            ),
            dict(
                name='supplier_entity_id',
                description='''
                    מספר התאגיד של הספק.
                    יכול להיות ריק אם הספק הוא אדם פרטי
                ''',
                sample_values=['570053512', '589906114', '513705699'],
                type='string',
                default=lambda row: row.get('entity_id')
            ),
            dict(
                name='supplier_entity_kind',
                description='''
                    סוג הארגון של הספק.
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
                default=lambda row: (row.get('entity_kind') or 'private_person')
            ),
        ],
        search=dict(
            index='contract-spending',
            field_map={
                'budget_code': 'nice-budget-code',
                'purpose': 'purpose',
                'purchasing_ministry': 'publisher_name',
                'purchasing_method': 'purchase_method',
                'volume': 'volume',
                'executed': 'executed',
                'order_date': 'order_date',
                'end_date': 'end_date',
                'start_year': 'min_year',
                'end_year': 'max_year',
                'is_active': 'contract_is_active',
                'supplier_entity_name': ['entity_name', 'supplier_name'],
                'supplier_entity_id': 'entity_id',
                'supplier_entity_kind': 'entity_kind',
            },
            filters={}
        )
    ),
    supports_data=dict(
        source='/var/datapackages/supports/by-request-year',
        description='''
            נתוני תמיכות תקציביות בחברות ועמותות.
            תמיכות תקציביות הן לא התקשרויות רכש, אלא ניתנות תחת קריטריונים מוגדרים היטב (״מבחני תמיכה״)
            המידע הוא לכלל שנות התקציב מאז 2004 ועד השנה הנוכחית (2024).
            חיפוש טקסט חופשי - לפי שם המשרד, ומטרת התמיכה *בלבד*!
            השתמש בשדות budget_code ו recipient_entity_id בשליפות ב-db ולא בשדה recipient_entity_name או purpose.
            לפני סינון לפי שדה supporting_ministry, בדוק את הערכים הזמינים (distinct query) כדי לבחור בצורה נכונה.
        ''',
        fields=[
            item_url('supports', ['budget_code', 'year_requested', 'short_id', 'request_type']),
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
                most_common_values=[
                    'מענק איזון לרשויות מקומיות',
                    'מענק לאינטל',
                    'תמיכה בקופות חולים בגין הסכמי ייצוב',
                    'בנייה חדשה של כיתות לימוד - כללי',
                    'תמיכה במוסדות תורניים',
                    'מענק בירה',
                    'תמיכה בקופות החולים בגין משבר הקורונה',
                    'חרבות ברזל- מענקים ייעודיים לרשויות',
                    'מעונות יום ומשפחתונים - סבסוד שהות ילדי',
                    'תכנית לאומית לחיזוק הרפואה הציבורית',
                    'סל נתצים - מאיץ',
                    'מבחני תמיכה בקופות חולים',
                    'סיוע בגין אי העלאת גיל פרישה',
                    'מגזרי המיעוטים',
                    'קרן לצמצום פערים ברשויות המקומיות',
                    'בנייה חדשה של גני ילדים',
                ],
                type='string',
                default=lambda row: row.get('support_title')
            ),
            dict(
                name='supporting_ministry',
                description='''
                    שם המשרד שמבצע את התמיכה.
                    חשוב לשים לב ששמות המשרדים משתנים ועשויים להפויע בצורות שונות בתמיכות שונות, לכדן כדאי להשתמש בשאילתת distinct לפני השימוש בשדה.
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
                    '3' + 'א',
                    'הקצבות',
                    'תמיכה דרך מוסד',
                    'תמיכה כלכלית',
                    'בקשת תמיכה לפרט',
                ],
                transform=lambda x: ('3' + 'א') if x == ('א' + '3') else x,
            ),
            dict(
                name='year',
                description='''
                    שנת  אישור התמיכה
                ''',
                sample_values=[2017, 2023, 2024],
                type='integer',
                default=lambda row: row.get('year_requested'),
            ),
            dict(
                name='amount_approved',
                description='''
                    הסכום שאושר במסגרת התמיכה.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                filter=lambda x: x is not None and x > 0
            ),
            dict(
                name='amount_paid',
                description='''
                    הסכום ששולם בפועל במסגרת התמיכה.
                ''',
                sample_values=[1000000, 5000000, 10000000],
                type='number',
                default=lambda row: row.get('amount_total') or 0,
            ),
            dict(
                name='recipient_entity_name',
                description='''
                    השם הרשמי של מקבל התמיכה (יכול להיות השם של חברה פרטית, עמותה, רשות מקומית וכד׳)
                ''',
                sample_values=['מ. א. מנשה', 'שירותי בריאות כללית', 'עירית אשקלון', 'מעון שירת הרך בע"מ'],
                type='string',
                default=lambda row: row.get('entity_name', row.get('recipient')),
                filter=lambda x: x is not None
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
                default=lambda row: (row.get('entity_kind') or 'private_person')
            ),
        ],
        search=dict(
            index='supports',
            field_map={
                'budget_code': 'nice-budget-code',
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
    budgetary_change_requests_data=dict(
        # source='/var/datapackages/budget/national/changes/with-transaction-id',
        source='/var/datapackages/budget/national/changes/full',
        description='''
            פרטי בקשות לשינויים בתקציב במהלך השנה.
            (נקראות גם ״פניות תקציביות״).
            אלו פניות (בקשות) אשר נשלחות לוועדת הכספים של הכנסת, לאישור או ליידוע.
            במאגר זה יש מידע על פניות שאושרו על ידי וועדת הכספים, כמו גם פניות אשר עדיין ממתינות לאישור.
        ''',
        fields=[
            item_url('national-budget-changes', ['transaction_id']),
            dict(
                name='year',
                description='''
                    השנה בה נשלחה הבקשה לוועדת הכספים.
                ''',
                sample_values=[2017, 2020, 2024],
                type='integer',
                default=lambda row: row.get('year')
            ),
            dict(
                name='transaction_id',
                description='''
                    מזהה ייחודי של הבקשה.
                    חובה לבקש את השדה הזה בכל שאילתה בכדי שתוכל לקבל פרטים נוספים על הבקשה ממאגר budgetary_change_transactions_data.
                ''',
                sample_values=['2005/01-001', '2017/60-003', '2011/18-028'],
                type='string',
                default=lambda row: row.get('transaction_id')
            ),
            dict(
                name='change_type',
                description='''
                    סוג הבקשה לשינוי בתקציב.
                ''',
                possible_values=[
                    'שינוי פנימי',
                    'לרזרבה כללית',
                    'עודפים',
                    'מרזרבה כללית',
                    'העברה לרזרבה',
                    'בין משרדים',
                    'קיצוץ לרזרבה',
                    'הקצאות מהרזרבה הכללית',
                    'הגדלה רגילה',
                    'קיצוץ',
                    'תקציב נוסף',
                    'המרת עב"צ לשכ"א',
                    'המרת עב"צ לשכ"א מהרזרבה הכללית',
                    'בין סעיפים',
                ],
                type='string',
                default=lambda row: ('בין משרדים' if len(row['change_title']) > 1 else row['change_title'][0]) if row.get('change_title') else None
            ),
            dict(
                name='committee_procedure',
                description='''
                    סוג האישור הנדרש לבקשה.
                    לעיתים וועדת הכספים בכנסת נדרשת לאשר את הבקשה.
                    במקרים אחרים, הוועדה מיודעת אך יכולה לדחות בקשה במידת הצורך.
                    בשאר המקרים השינוי מאושר על ידי אגף התקציבים במשרד האוצר, והוועדה רק מיודעת על השינוי שבוצע.
                    אם לא התבקש במפורש סוג אישור מסוים, אין לסנן לפי שדה זה.
                ''',
                possible_values=[
                    'אישור ועדה',
                    'הודעה לועדה',
                    'אישור אגף',
                ],
                type='string',
                default=lambda row: row.get('change_type_name')[0]
            ),
            dict(
                name='is_pending',
                description='''
                    האם הבקשה עדיין ממתינה לאישור.
                ''',
                possible_values=[True, False],
                type='boolean',
                default=lambda row: row.get('pending')[0]
            ),
            dict(
                name='request_title',
                description='''
                    כותרת הבקשה לשינוי בתקציב.
                ''',
                sample_values=['עודפים מחוייבים', 'יישום החלטת ממשלה 1364', 'קיצוץ רוחבי דרוזים'],
                type='string',
                default=lambda row: ', '.join(row.get('req_title') or [])
            ),
            dict(
                name='request_explanation',
                description='''
                    פירוט הבקשה לשינוי בתקציב.
                ''',
                type='string',
                default=lambda row: row.get('explanation')
            ),
            dict(
                name='request_summary',
                description='''
                    מתצית השינויים המבוקשים, מסוכמים לרמת משרד.
                    לקבלת מידע מפורט יותר, יש לתשאל את מאגר budgetary_change_transactions_data עם השדה transaction_id של הבקשה.
                ''',
                type='object',
                sample_values=[
                    {
                        'from': [{
                            'code': '23',
                            'office': 'משרד הבריאות',
                            'amount': -1000000,
                        }],
                        'to': [{
                            'code': '20',
                            'office': 'משרד החינוך',
                            'amount': 1000000,
                        }],
                    }
                ],
                default=convert_request_summary
            ),
            dict(
                name='appproval_date',
                description='''
                    תאריך אישור הבקשה, במידה והיא אושרה.
                ''',
                sample_values=['2021-01-01', '2023-12-31', '2024-02-29'],
                type='date',
                default=lambda row: (row.get('date') or [None])[0]
            ),
            dict(
                name='committee_ids',
                description='''
                    מספרי הפניות לוועדת הכספים, במידה והשינויים כללו כאלו.
                ''',
                sample_values=[[9002], [67, 68], [1001, 1002]],
                type='array',
                default=lambda row: row.get('committee_id') or []
            ),
        ],
        search=dict(
            index='national-budget-changes',
            field_map={
                'year': 'year',
                'transaction_id': 'transaction_id',
                'change_type': 'change_title',
                'committee_procedure': 'change_type_name',
                'is_pending': 'is_pending',
                'request_title': 'req_title',
                'request_explanation': 'explanation',
                'request_summary': 'summary',
                'approval_date': 'date',
                'committee_ids': 'committee_id',
            },
        )
    ),
    budgetary_change_transactions_data=dict(
        source='/var/datapackages/budget/national/changes/with-transaction-id',
        resources='national-budget-changes',
        # source='/var/datapackages/budget/national/changes/full',
        description='''
            פרטי השינויים הספיפיים בסעיפי תקציב במהלך השנה.
            כל בקשה לשינוי כוללת מספר שינויים ספציפיים.
            (הבקשות עצמן מתוארות במאגר budgetary_change_requests_data).
            במאגר זה יש מידע על שינויים שכבר אושרו על ידי וועדת הכספים, כמו גם שינויים אשר עדיין ממתינים לאישור.
            כל שינוי מקושר לבקשה בו הוא מופיע דרך השדה transaction_id.
            כאשר מבקשים פרטי העברה מסוימת, יש למבצע שאילתה על מאגר זה ולסנן עם transaction_id.
        ''',
        fields=[
            dict(
                name='item_url',
                description='''
                    קישור לדו״ח מפורט על התכניתה התקציבית באתר מפתח התקציב.
                ''',
                type='string',
                default=lambda row: f'https://next.obudget.org/dashboards/budget-transfers/budget/{row["budget_code"]}/{row["year"]}'
            ),
            dict(
                name='year',
                description='''
                    השנה בה נשלחה לוועדת הכספים הבקשה שכוללת את השינוי הזה.
                ''',
                sample_values=[2017, 2020, 2024],
                type='integer',
                default=lambda row: row.get('year')
            ),
            dict(
                name='code',
                description='''
                    מזהה הסעיף התקציבי שבו נעשה השינוי.
                    (נקרא גם ״קוד התכנית התקציבית״).
                    כשמציגים את רשימת השינויים בבקשת העברה, יש להציג תמיר את השדה הזה.
                ''',
                sample_values=['20.67.01', '26.13.01', '19.42.02'],
                type='string',
                default=lambda row: clean_budget_code(row.get('budget_code'))
            ),
            dict(
                name='title',
                description='''
                    כותרת הסעיף התקציבי שבו נעשה השינוי.
                    (נקרא גם ״שם התכנית התקציבית״).
                    כשמציגים את רשימת השינויים בבקשת העברה, יש להציג תמיר את השדה הזה.
                ''',
                sample_values=['תכנון ובנייה', 'משרד הבריאות', 'משרד החינוך'],
                type='string',
                default=lambda row: row.get('budget_title')
            ),
            dict(
                name='transaction_id',
                description='''
                    מזהה ייחודי של הבקשה בה מופיע השינוי.
                ''',
                sample_values=['2005/01-001', '2017/60-003', '2011/18-028'],
                type='string',
                default=lambda row: row.get('transaction_id')
            ),
            dict(
                name='is_pending',
                description='''
                    האם השינוי עדיין ממתין לאישור.
                ''',
                possible_values=[True, False],
                type='boolean',
                default=lambda row: row.get('pending')
            ),
            dict(
                name='appproval_date',
                description='''
                    תאריך אישור השינוי, במידה והוא אושר.
                ''',
                sample_values=['2021-01-01', '2023-12-31', '2024-02-29'],
                type='date',
                default=lambda row: row.get('date')
            ),
            dict(
                name='diff__amount_allocated',
                description='''
                    השינוי בתקציב שהוקצה לסעיף.
                ''',
                sample_values=[1000000, -5000000, 10000000],
                type='number',
                default=lambda row: row.get('net_expense_diff')
            ),
        ],
        search='n/a'
    ),
)

def remove_pk(package: DF.PackageWrapper):
    for res in package.pkg.descriptor['resources']:
        schema = res.get('schema', {})
        if 'primaryKey' in schema:
            del schema['primaryKey']
    yield package.pkg
    yield from package

def get_flow(table, params, debug=False):
    steps = []
    source = debug_source(params['source'], debug)
    description = clean_lines(params['description'])
    fields = params['fields']
    search = params.get('search')
    if search and isinstance(search, dict) and search.get('field_map'):
        search['field_map']['item_url'] = 'item-url'
    resources = params.get('resources')
    steps.append(DF.load(f'{source}/datapackage.json', resources=resources, limit_rows=10000 if debug else None))
    steps.append(DF.update_resource(-1, description=description, name=table, search=search))
    
    if not debug:
        steps.append(remove_pk)

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
    for table, params in reversed(PARAMETERS.items()):
        get_flow(table, params).process()
    return DF.Flow(
        [dict(done=True)],
        DF.update_resource(-1, **{'dpp:streaming': True}),    
    )


if __name__ == '__main__':
    params = PARAMETERS['budgetary_change_transactions_data']
    DF.Flow(
        get_flow('budgetary_change_transactions_data', params, debug=True),
    ).process()
