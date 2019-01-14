from pyquery import PyQuery as pq
from datetime import datetime


APPOINTMENT_DIRECTOR = "ת093" # דוח מיידי על מינוי דירקטור (שאינו תאגיד) או יחיד המכהן מטעם תאגיד שהוא דירקטור בחברה פרטית
APPOINTMENT_VIP = "ת091" #דוח מיידי על מינוי נושא משרה בכירה (למעט מינוי דירקטור/ יו"ר דירקטוריון ולמעט יחיד שמונה מטעם תאגיד שהוא דירקטור)
APPOINTMENT_ACCOUNTANT = "ת090" #דוח מיידי על מינוי רואה חשבון

RETIRED_VIP = "ת094" #דוח מיידי על נושא משרה בכירה שחדל לכהן בתפקידו
RETIRED_ACCOUNTANT = "ת095" #דוח מיידי על רואה חשבון שחדל לכהן בתפקידו
BOARD_MEETING = "ת460" #דוח מיידי על אסיפה

BOARD_MEETING_RESULTS = "ת049" # דוח מיידי על תוצאות אסיפה לאישור עסקה עם בעל שליטה ו/או לאישור הצעה פרטית ו/או אישור כפל כהונה יו"ר מנכ"ל ו/או מינוי דח"צ.
OTHER_IMMEDIATE_REPORT = "C002" #an Immediate report
IMMEDIATE_REPORT = "ת121" #דוח מיידי

OTHER = "C003" # Other Report or Announcement


class ParseError(Exception):
    def __init__(self):
        super(ParseError, self).__init__("Failed to parse document")


def _wrap_parse_error(f):
    def wrapped(*args):
        try:
            return f(*args)
        except ParseError as err:
            raise err
        except Exception as err:
            raise ParseError() from err
    return wrapped


def _findByTextAlias(e, aliaes):
    selector = ",".join(["[fieldalias={}]".format(a) for a in aliaes])
    return e.find(selector)


class MayaForm(object):

    def __init__(self, url):
        self._page = pq(url, parser='html')

    @property
    @_wrap_parse_error
    def id(self):
        'This method works with new documents and old documents that do not support #HeaderProofValue'
        elem = self._page.find('#HeaderProof ~ span:first')
        if len(elem)==0:
            raise ValueError("Could not find אסמכתא in form")
        elif len(elem)>1:
            raise ValueError("Found multiple אסמכתאs in form")
        return pq(elem[0]).text().strip()


    @property
    @_wrap_parse_error
    def company(self):
        elem = self._page.find('#HeaderEntityNameEB')
        if len(elem)==0:
            raise ValueError("Could not find company name in form")
        elif len(elem)>1:
            raise ValueError("Found multiple company names in form")
        return pq(elem[0]).text().strip()

    @property
    @_wrap_parse_error
    def type(self):
        elem = self._page.find('#HeaderFormNumber')
        if len(elem)==0:
            raise ValueError("Could not find form type in form")
        elif len(elem)>1:
            raise ValueError("Found multiple form types in form")
        return pq(elem[0]).text().strip()

    @property
    @_wrap_parse_error
    def fix_for(self):
        elem = self._page.find('#HeaderFixtReport')
        if len(elem)==0:
            return None
        link = pq(elem[0]).find("#HeaderProofFormat")
        if len(link)==0:
            raise ValueError("Could not find form replacement")
        elif len(link)>1:
            raise ValueError("Found multiple form replacements")
        return pq(link[0]).text().strip()

    @property
    @_wrap_parse_error
    def is_nomination(self):
        return self.type in [APPOINTMENT_DIRECTOR, APPOINTMENT_VIP, APPOINTMENT_VIP]

    @property
    @_wrap_parse_error
    def position_start_date(self):
        aliases = ['TaarichTchilatHaCehuna', 'TaarichTchilatCehuna', 'TaarichTehilatCehuna',
                          'TaarichTchilatHaKehuna', 'TaarichTchilatKehuna', 'TaarichTehilatKehuna']
        elem = _findByTextAlias(self._page, aliases)
        if len(elem)==0:
            raise ValueError("Could not find position start date in form")
        elif len(elem)>1:
            raise ValueError("Found multiple  position start date in form")
        return datetime.strptime(pq(elem[0]).text().strip(), '%d/%m/%Y')

    @property
    @_wrap_parse_error
    def positions(self):
        aliases = ['Tapkid', 'Tafkid', 'HaTafkidLoMuna']
        desc_aliases = ['TeurTafkid', 'LeloTeur', 'TeurHaTafkidLoMuna']
        elems = _findByTextAlias(self._page, aliases)

        if len(elems)==0 and self.type == APPOINTMENT_ACCOUNTANT:
            return ["רואה חשבון"]
        if len(elems)==0:
            raise ValueError("Could not find position in form")

        def extract_title(elem):
            txt = elem.text().strip()
            if txt is None or txt in [':', 'אחר']:
                row = elem.closest('tr')
                aliases = _findByTextAlias(pq(row[0]), desc_aliases)
                if len(aliases)==0:
                    raise ValueError("could not find position description in form")
                elif len(aliases)>1:
                    raise ValueError("Found multiple position descriptions in form")
                return pq(aliases[0]).text().strip()
            return txt

        return [extract_title(pq(it)) for it in elems]

    @property
    @_wrap_parse_error
    def gender(self):
        aliases = ['Gender', 'Min', 'gender']
        elem = _findByTextAlias(self._page, aliases)
        if len(elem)==0:
            return ""
        elif len(elem)>1:
            raise ValueError("Found multiple gender in form")
        gender = pq(elem[0]).text().strip()
        if gender.startswith("ז"):
            return "man"
        return "woman"

    @property
    @_wrap_parse_error
    def full_name(self):
        elem = _findByTextAlias(self._page, ['Accountant'])
        if len(elem)==1:
            return pq(elem[0]).text().strip()
        aliases = ['Shem', 'ShemPratiVeMishpacha', 'ShemPriatiVeMishpacha', 'ShemMishpahaVePrati',
                   'ShemRoeCheshbon', 'ShemRoehHeshbon']
        elem = _findByTextAlias(self._page, aliases)
        if len(elem)==0:
            raise ValueError("Could not find full name in form")
        elif len(elem)>1:
            raise ValueError("Found multiple full names in form")
        return pq(elem[0]).text().strip()

