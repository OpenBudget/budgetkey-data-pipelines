from datapackage_pipelines_budgetkey.pipelines.people.company_appointments.maya.maya_nomination_form import MayaForm, ParseError
from urllib.parse import urljoin
from datetime import datetime

def get_url(obj_name):
    return urljoin("https://ams3.digitaloceanspaces.com/budgetkey-files/", obj_name)


def test_form_extract_id():
    # Use a Regular example with HeaderProofValue
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235402.htm")).id == "2007-01-001909"

    # Use a file that does not contain HeaderProofValue
    assert MayaForm(get_url("maya.tase.co.il/2006_11/223729.htm")).id == "2006-01-135211"

    # Use a file that has the HeaderProofValue in a seperate table cell from HeaderProof
    assert MayaForm(get_url("maya.tase.co.il/2017_01/1074729.htm")).id == "2017-01-000544"


def test_form_extract_company_name():
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235402.htm")).company == 'מת"ב - מערכות תקשורת בכבלים בע"מ'


def test_form_extract_fix_for():
    # Use a file that does not contain a Fix For
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235402.htm")).fix_for is None

    # A file that does contain a fix for field
    assert MayaForm(get_url("maya.tase.co.il/2007_01/234967.htm")).fix_for == "2007-01-000229"


def test_form_extract_type():
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235402.htm")).type == 'ת091'


def test_form_extract_position_start_date():
    #Form Uses the alias TaarichTchilatHaCehuna
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235392.htm")).position_start_date  \
           == datetime.strptime("01/01/2007", "%d/%m/%Y")

    #Form Uses the alias TaarichTchilatHaKehuna
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235402.htm")).position_start_date \
           == datetime.strptime("01/01/2007", "%d/%m/%Y")

    #Form Uses the alias TaarichTchilatKehuna
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235356.htm")).position_start_date \
           == datetime.strptime("29/12/2003", "%d/%m/%Y")

    #Form Uses the alias TaarichTehilatCehuna
    assert MayaForm(get_url("maya.tase.co.il/2010_05/537834.htm")).position_start_date \
           == datetime.strptime("28/03/2005", "%d/%m/%Y")

    #Form Uses the alias TaarichTehilatKehuna
    assert MayaForm(get_url("maya.tase.co.il/2006_08/206625.htm")).position_start_date \
           == datetime.strptime("01/07/2004", "%d/%m/%Y")


def test_form_extract_gender():
    #Empty values do not fail
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235402.htm")).gender == ""

    #Empty values in the form of "______" fo not fail
    assert MayaForm(get_url("maya.tase.co.il/2014_05/896658.htm")).gender == ""

    #Example of value of male
    assert MayaForm(get_url("maya.tase.co.il/2015_01/941675.htm")).gender == "man"

    #Example of value for woman
    assert MayaForm(get_url("maya.tase.co.il/2015_01/941652.htm")).gender == "woman"


def test_form_extract_full_name():
    #Form Users the alias 'Shem'
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235402.htm")).full_name == "ברגמן דלי"

    #Form Users the alias 'ShemPriatiVeMishpacha'
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235392.htm")).full_name == "צ'צ'יק ישראל"

    #Form Users the alias 'ShemMishpahaVePrati'
    assert MayaForm(get_url("maya.tase.co.il/2010_05/537834.htm")).full_name == "צימר יוסף"


def test_form_extract_positions():

    # Form contains a single Job
    assert MayaForm(get_url("maya.tase.co.il/2007_01/235402.htm")).positions == ['חשב']

    # Form contains multiple job listings
    assert MayaForm(get_url("maya.tase.co.il/2006_12/233330.htm")).positions == ['סגן מנהל כללי', 'חברת הנהלה', 'מנהלת מערך משאבי אנוש ומינהל']

    # Form contains a job listing from two strings"
    assert MayaForm(get_url("maya.tase.co.il/2008_12/408859.htm")).positions == ['מנהל כללי', 'נושא משרה בכירה בתאגיד נשלט בעל השפעה מהותית על התאגיד: מנכ"ל אסים נדל"ן בע"מ, חברה בת של החברה (להלן: "אסים נדל"ן")']

