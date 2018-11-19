import logging

from datapackage_pipelines.wrapper import ingest, spew
from datetime import datetime

parameters, dp, res_iter = ingest()

POSITION_MAP = {
    "CEO": ["מנכ \"ל", "מנ כ\"לית בפועל",  "מנכ\"ל", "מנכ\"לית", "מנ כ\"ל", "מנכ\"לית בפועל", "מנכ\"ל בפועל", "מנ כ\"ל בפועל", "מנ כ\"לית"],
    "Chairperson": ["יו\"רית", "יו\"ר", "י ו\"ר", "י ו\"רית",],
    "Board member": ["דירקטור /חבר",  "דירקטור", "דירקטור/חבר", "דיקטורית", "דח\"צ",  "דירקטורית", "דח\"צית",]
}

GENDER_MAP = {
    "man": ["מנכ \"ל", "מנכ\"ל",  "יו\"ר", "י ו\"ר", "דירקטור /חבר",  "דירקטור", "דירקטור/חבר", "מנ כ\"ל", "מנכ\"ל בפועל", "דח\"צ", "מנ כ\"ל בפועל",],
    "woman": ["מנ כ\"לית בפועל", "יו\"רית", "דיקטורית", "מנכ\"לית", "מנכ\"לית בפועל",  "דירקטורית", "דח\"צית", "י ו\"רית", "מנ כ\"לית"]
}

FIX_COMPANY_NAME = {
    'חברת נמל חיפה בע"מגולדשטיין':'חברת נמל חיפה בע"מ',
    'חברת נתיבי תחבורה עירוניים (נת"ע)דקל':'חברת נתיבי תחבורה עירוניים (נת"ע)',
    'רפאל - מערכות לחימה מתקדמות בע"מ':'חברת רפאל - מערכות לחימה מתקדמות בע"מ',
    'מתימו "פ ­ מרכז התעשייה הישראלית למחקר ופיתוח':'מתימו"פ - מרכז התעשייה הישראלית למחקר ופיתוח',
    ')חל "צ(החברה למתנ"סים מרכזים קהילתיים בישראל בע"מ':')חל"צ(החברה למתנ"סים מרכזים קהילתיים בישראל בע"מ',
    'החברה לחינוך ימי בישראל (בתיה"ס הימיים)':'החברה לחינוך ימי בישראל (בי"ס ימיים)',
    'רותם אינק.':'.רותם אינק',
    'רותם גמב"ה':'.רותם אינק',
    'ק .ל.ע -. חברה לניהול קרן השתלמות לעובדים סוציאליים בע"מ':'ק.ל.ע -. חברה לניהול קרן השתלמות לעובדים סוציאליים בע"מ',
    'קנט - קרן לביטוח נזקי טבע בחקלאות בע"מ':'קנט ­ קרן לביטוח נזקי טבע בחקלאות בע"מ',
    'קרנות השתלמות למורים תיכוניים , סמינרים ומפקחים - חברה מנהלת בע"מ':'קרנות השתלמות למורים תיכוניים, סמינרים ומפקחים - חברה מנהלת בע"מ',
    'תומר  חברה ממשלתית בע"מ':'תומר בע"מ',
    ')בי "ס ימיים(החברה לחינוך ימי בישראל':')בי"ס ימיים(החברה לחינוך ימי בישראל',


}


# For now I keep a manual table. It looks like errors are rare and I prefer this will fail and get a human to fix
# if errors happen often then change
FIX_DATE = {
    '12/092021':'12/09/2021'
}

def trim_list(arr):
    try:
        st = next(i for i,v in enumerate(arr) if v)
        et = len(arr) - next(i for i,v in enumerate(reversed(arr)) if v)
        return arr[st:et]
    except StopIteration:
        return []


def parse_position(val):
    try:
        return next(k for k,v in POSITION_MAP.items() if val in v)
    except StopIteration:
        raise ValueError("Unable to parse Position. Value {%s} is unknown" % val)


def parse_gender(val):
    try:
        return next(k for k,v in GENDER_MAP.items() if val in v)
    except StopIteration:
        raise ValueError("Unable to parse Gender. Value {%s} is unknown" % val)



def process_rows_in_resource(resource):
    company = ''
    for row in resource:
        pdf_record = row['data']
        url = row['url']
        try:
            trimmed_pdf_record = trim_list(pdf_record)

            if len(trimmed_pdf_record) == 0:
                continue

            if "דירקטורים מכהנים בחברות" in trimmed_pdf_record[0]:
                continue

            if len(trimmed_pdf_record) in [1,2]:
                company = trimmed_pdf_record[0]
                if len(trimmed_pdf_record) == 2:
                    company = trimmed_pdf_record[1] + ' ' + trimmed_pdf_record[0]

                if company in FIX_COMPANY_NAME:
                    company = FIX_COMPANY_NAME[company]
            else:
                if trimmed_pdf_record[2] == 'תפקיד':
                    continue
                new_row = {k:v for k,v in row.items() if k != 'data'}
                new_row['company'] = company

                new_row['last_name'] = trimmed_pdf_record[0]
                new_row['first_name'] = trimmed_pdf_record[1]

                new_row['position'] = parse_position(trimmed_pdf_record[2])
                new_row['gender'] = parse_gender(trimmed_pdf_record[2])

                if len(trimmed_pdf_record) >3:
                    date_str = FIX_DATE.get(trimmed_pdf_record[3], trimmed_pdf_record[3])
                    new_row['expected_end_date'] = datetime.strptime(date_str, "%d/%m/%Y")
                yield new_row
        except Exception as e:
            raise ValueError("Failed parsing file {} ( {})".format(url, pdf_record)) from e






def modify_datapackage(datapackage):
    resource = datapackage['resources'][0]
    resource['schema']['fields'] = [field for field in resource['schema']['fields'] if field['name'] != 'data']
    resource['schema']['fields'].extend([
        {"name": "company", "type": "string"},
        {"name": "expected_end_date", "type": "date"},
        {"name": "position", "type": "string"},
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "gender", "type":"string"}
    ])

    return datapackage


spew(modify_datapackage(dp), [process_rows_in_resource(res) for res in res_iter])

