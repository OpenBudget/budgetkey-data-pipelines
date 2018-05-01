from datapackage_pipelines_budgetkey.processors.fingerprint import calc_fingerprint


def test_simple_text_stays_the_same():
    assert calc_fingerprint("simple") == "simple"
    assert calc_fingerprint("דניה סיבוס") == "דניה סיבוס"


def test_extra_spaces():
    assert calc_fingerprint("simple ") == calc_fingerprint("simple")
    assert calc_fingerprint(" simple ") == calc_fingerprint("simple")


def test_empty_input():
    assert calc_fingerprint("") == "<empty>"
    assert calc_fingerprint(" ") == "<empty>"


def test_dashes():
    assert calc_fingerprint("רמת גן") == calc_fingerprint("רמת-גן")


def test_capital_case():
    assert calc_fingerprint("Jerusalem") == calc_fingerprint("JERUSALEM")
    assert calc_fingerprint("Jerusalem") == calc_fingerprint("jerusalem")


def test_incorporated_suffix():
    assert calc_fingerprint("דניה סיבוס") == calc_fingerprint("דניה סיבוס בעמ")
    assert calc_fingerprint('דניה סיבוס') == calc_fingerprint('דניה סיבוס בע"מ')
    assert calc_fingerprint('דניה סיבוס') == calc_fingerprint('דניה סיבוס בע"מ')
    assert calc_fingerprint('דניה סיבוס') == calc_fingerprint("דניה סיבוס בע'מ")
    assert calc_fingerprint("דניה סיבוס") == calc_fingerprint("דניה סיבוס - בעמ")


def test_regional_council():
    assert calc_fingerprint("מטה יהודה") == calc_fingerprint("מועצה אזורית מטה יהודה")
    assert calc_fingerprint("מטה יהודה") == calc_fingerprint("מ.א. מטה יהודה")
    assert calc_fingerprint("מטה יהודה") == calc_fingerprint('מוא"ז מטה יהודה')
    assert calc_fingerprint("מטה יהודה") == calc_fingerprint("מועצה איזורית מטה יהודה")


def test_city_council():
    assert calc_fingerprint("ירושלים") == calc_fingerprint("עיריית ירושלים")
    assert calc_fingerprint("ירושלים") == calc_fingerprint("עירית ירושלים")


def test_all_numbers():
    assert calc_fingerprint("334343") == "334343"



def test_special_cases():
    assert calc_fingerprint("מועדון חבר") == calc_fingerprint("מועדון")
    assert calc_fingerprint("334343-334343") == "334343 334343"