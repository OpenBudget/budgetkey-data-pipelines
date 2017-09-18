from datapackage_pipelines.wrapper import process
import logging


IS_FIX_STARTS_WITH = [
    "תיקון ל",
     "תיקון ",
     "תיקונים ל",
]

# matches and populates "what" and "office" fields
# office is followed directly after this string and matches to OFFICE_ALIASES below and office field in source row
WHAT_OFFICE_STARTS_WITH = [
    "מבחנים לחלוקת כספי תמיכות בדרך של הלוואה של ",
             'מבחנים לחלוקת כספים לצורך תמיכה של ',
        "מבחנים לחלוקת כספי תמיכות לצורך תמיכה של ",
                     "מבחנים לחלוקת כספי תמיכה של ",
                    "מבחנים לחלוקת כספי תמיכות של ",
                   'מבחנים לחלוקת כספי התמיכות של ',
                          "מבחנים לחלוקת כספים של ",
                         "מבחנים לחלוקת תמיכות של ",
                            "חלוקת כספי תמיכות של ",
                            "מחנים למתן תמיכות של ",
                             'מבחן למתן תמיכות של ',
                           "מבחנים לצורך תמיכה של ",
                           "מבחנים למתן תמיכות של ",
                                "מבחנים לתמיכה של ",
                                   'מבחן תמיכה של ',
                                  "מבחני תמיכה של ",
                                    "מתן תמיכה של ",
                                       "מבחנים של ",
]

# matches "what" without "office"
WHAT_STARTS_WITH = [
    "חלוקת כספי תמיכות בדרך של הלוואה  לתמיכה ב",
    "מבחנים לחלוקת כספי תמיכה ב",
    'מבחני לחלוקת כספי תמיכה ב',
    'מבחן תמיכה משלימה ב',
    'מבחני תמיכה ל',
    "מבחן תמיכה ל",
    'מבחן תמיכה ב',
    "סיוע ותמיכה ב",
    'מבחנים ל',
    "תמיכה ב",
]

FOR_PURPOSE_STARTS_WITH = [
    'לצורך תמיכה ב',
     'למתן תמיכה ב',
          'תמיכה ב',
         'לתמיכה ב',
           'לצורך ',
            'עבור ',
                'ל',
                'ב',
]


# additional office names to try
# to support mismatch between the original office field and the office name as it appears in the title
OFFICE_ALIASES = [
    'משרד הרווחה והשירותים החברתיים',
     'משרד התעשייה המסחר והתעסוקה',
    'משרד התעשייה, המסחר והתעסוקה',
       'משרד המדע הטכנולוגיה והחלל',
             'המשרד לאזרחים וותיקים',
              'משרד התרבות והספורט',
                 'הרשות לפיתוח הגליל',
                  'הרשות לפיתוח הנגב',
                      'משרד התרבות',
                       'משרד הרווחה',

] + [s.strip() for s in [
    # scraped by going to http://www.pmo.gov.il/IsraelGov/Pages/GovMinistries.aspx
    # running this in console:
    # govOffices=[]; jQuery("td.gvContent").each(function(i, elt){govOffices.push(jQuery(elt).text())})
    " משרד ראש הממשלה", " משרד החוץ", " משרד הביטחון", " משרד האוצר", " משרד החינוך", " משרד המשפטים", " משרד הבריאות", " משרד הפנים", " משרד הכלכלה והתעשייה", " משרד התחבורה והבטיחות בדרכים", " המשרד לביטחון הפנים", " משרד העלייה והקליטה", " המשרד לשיתוף פעולה אזורי", " משרד התשתיות הלאומיות, האנרגיה והמים", " המשרד לפיתוח הפריפריה, הנגב והגליל", " משרד התרבות והספורט", " משרד המדע, הטכנולוגיה והחלל", " משרד החקלאות ופיתוח הכפר", " משרד הבינוי והשיכון", " משרד העבודה, הרווחה והשירותים החברתיים", " משרד התיירות", " משרד התקשורת", "המשרד לענייני מודיעין ", "המשרד לנושאים אסטרטגיים והסברה", " המשרד לשירותי דת", " המשרד לשוויון חברתי", " משרד התפוצות", " המשרד להגנת הסביבה", "המשרד לירושלים ומורשת"
]] + [s.strip().strip("ה") for s in [
    # duplication of above list but removing another prefix
    " משרד ראש הממשלה", " משרד החוץ", " משרד הביטחון", " משרד האוצר", " משרד החינוך", " משרד המשפטים", " משרד הבריאות", " משרד הפנים", " משרד הכלכלה והתעשייה", " משרד התחבורה והבטיחות בדרכים", " המשרד לביטחון הפנים", " משרד העלייה והקליטה", " המשרד לשיתוף פעולה אזורי", " משרד התשתיות הלאומיות, האנרגיה והמים", " המשרד לפיתוח הפריפריה, הנגב והגליל", " משרד התרבות והספורט", " משרד המדע, הטכנולוגיה והחלל", " משרד החקלאות ופיתוח הכפר", " משרד הבינוי והשיכון", " משרד העבודה, הרווחה והשירותים החברתיים", " משרד התיירות", " משרד התקשורת", "המשרד לענייני מודיעין ", "המשרד לנושאים אסטרטגיים והסברה", " המשרד לשירותי דת", " המשרד לשוויון חברתי", " משרד התפוצות", " המשרד להגנת הסביבה", "המשרד לירושלים ומורשת"
]]


def parse_row(row):
    parsed_row = {
        "is_fix": None,
        "what": "",
        "for_purpose": "",
        "purpose": "",
        "parsed_office": "",
        "warning": ""
    }
    title = row["title"].strip().strip("\u200b-")
    if len(title) < 5:
        parsed_row["warning"] = "title is too short"
        return parsed_row
    else:
        title = parse_is_fix(parsed_row, title)
        what_required = is_what_required(title)
        title, what_matched = parse_what(parsed_row, row, title)
        if what_required and not what_matched:
            raise Exception("{}; {}".format(parsed_row, row))
        title = title.strip()
        title = parse_office(parsed_row, row, title)
        parse_purpose(parsed_row, title)
        return parsed_row

def parse_office(parsed_row, row, title):
    matching_offices = [o for o in [row["office"]] + OFFICE_ALIASES if title.startswith(o)]
    if len(matching_offices) > 0 and parsed_row["parsed_office"] == "":
        parsed_row["parsed_office"] = matching_offices[0]
        title = title[len(matching_offices[0]):]
    return title

def parse_purpose(parsed_row, title):
    matching_for_purposes = [p for p in FOR_PURPOSE_STARTS_WITH if title.startswith(p)]
    if len(matching_for_purposes) < 1:
        parsed_row["for_purpose"] = ""
        parsed_row["purpose"] = title.strip()
    else:
        parsed_row["for_purpose"] = matching_for_purposes[0].strip()
        parsed_row["purpose"] = title[len(matching_for_purposes[0]):].strip()


def parse_what(parsed_row, row, title):
    what_office_matching_prefixes = [p for p in WHAT_OFFICE_STARTS_WITH if title.startswith(p)]
    what_matched = False
    if len(what_office_matching_prefixes) < 1:
        # failed to match what and office
        # fallback to match only what
        what_matching_prefixes = [p for p in WHAT_STARTS_WITH if title.startswith(p)]
        if len(what_matching_prefixes) < 1:
            parsed_row["warning"] = "failed to find matching what prefix"
        else:
            parsed_row["what"] = what_matching_prefixes[0].strip()
            title = title[len(what_matching_prefixes[0]):]
            what_matched = True
    else:
        parsed_row["what"] = what_office_matching_prefixes[0].strip()
        title = title[len(what_office_matching_prefixes[0]):]
        matching_offices = [o for o in [row["office"]] + OFFICE_ALIASES if title.startswith(o)]
        if len(matching_offices) == 0:
            parsed_row["warning"] = "failed to match office"
        else:
            parsed_row["parsed_office"] = matching_offices[0].strip()
            title = title[len(matching_offices[0]):]
            what_matched = True
    return title, what_matched


def is_what_required(title):
    # any title that starts with מבחנים ; מבחני ; מבחן etc..
    # we will raise Exception if parser fails to match it
    return title.startswith("מבח")

def parse_is_fix(parsed_row, title):
    is_fix_matching_prefixes = [p for p in IS_FIX_STARTS_WITH if title.startswith(p)]
    parsed_row["is_fix"] = False
    if is_fix_matching_prefixes:
        parsed_row["is_fix"] = True
        title = title[len(is_fix_matching_prefixes[0]):]
    return title


def modify_datapackage(datapackage, parameters, stats):
    assert len(datapackage["resources"]) == 1
    datapackage["resources"][0]["schema"]["fields"] += [
        {"name": "is_fix", "type": "boolean",
         "description": "fix for an existing support?"},
        {"name": "what", "type": "string",
         "description": "what is the required support"},
        {"name": "purpose", "type": "string",
         "description": "purpose of this support / where / how will it be used"},
        {"name": "parsed_office", "type": "string",
         "description": "office parsed from the title"},
        {"name": "warning", "type": "string",
         "description": "warning/s from the parser"}
    ]
    return datapackage


def process_row(row, row_index, spec, resource_index, parameters, stats):
    return dict(parse_row(row), **row)


if __name__ == "__main__":
    process(modify_datapackage, process_row)
