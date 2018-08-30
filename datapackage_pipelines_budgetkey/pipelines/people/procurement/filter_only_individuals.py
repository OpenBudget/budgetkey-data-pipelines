import logging
import re
from datapackage_pipelines_budgetkey.common.is_valid_israeli_id import is_valid_israeli_id
from datapackage_pipelines.wrapper import process

filter_company_ids = [
    '0341578', #World health Organization
    '939072492', #מרכז רפואי ע"ש חיים שיבא
    '951020239', #בית חולים ברזילי, אשקלון
    '59857888', #ועד עובדי משרד הכלכלה
    '380000000', #WORLD TRADE ORGANIZATION
    '21998', #EUROPEAN BANK FOR RECONSTRUCTION
    '229706163', #ECMWF
    '3600020', #EMBASSY OF ISRAEL AUSTRALIA,
    '000000299',  #REUTERS AMERICA LLC.
    '000000059', #BLOOMBERG L.P.
    '784228454', #ICOMOS,
    '00010504', #QUIK PARK
    '73007726', #EPRA
    '80000003', #ועד פרקליטות מחוז דרום
    '0018', #ועד פרקליטות מחוז ת"א - פלילי
    '941079204', #Foreign Minisry-Payment in foreign
    '12734737', #חבר ועדה
    '0612', #NEW ENGLAND JOURNAL OF MEDICINE
    '1550565', #UBM UNITED BUSINESS MEDIA
    '371781477', #LPC REALTY ADVISORS I LP AAF
    '32128', #TIS-TELDAN INFORMATION SYSTEM
    '10017', #Ministry of Finance, New York
    '110000163', #Sage Publications
    '52778024', #נציג/ה בועדת חשודים
    '27805951', #בודק/ת עבודות גמר/בחינות בגרות
    '000910810', #CAMBRIDGE GOVERNANCE ADVISORS,
    '1040', #European Commission
    '00010264', #GRISTEDE S
    '31052012', #INTERNATIONAL ASSOCIATION OF CANCER
    '3600004', #EMBASSY OF ISRAEL STOCKHOLM
    '00000042', #TIME WARNER CABLE OF NYC
    '000000992', #EMBASSY-WASHINGTON DC
    '160878492', #WILLIAM HEIN & CO
    '101253', #EUROPEAN A VIATION SAFETY AGENCY
    '820975506', #SKYLINE RETAIL ACQUISITION I LLC
    '742270226', #TEXAS A&M ENGINEERING EXTENSION SER
    '773851001', #ACTION GLOBAL
    '50735307', #CENTRAL EUROPEAN GAZ HUB AG
    '265103275', #AMI AGRARMARKT INFORMATIONS GMBH
    '107510570', #INTENATIONAL AIR TRANSPORT ASSOC IA
    '00010280', #UNITED AIRLINES
    '429585680', #EUREKA SECRETARIAT AISBL
    '775673536', #El Al
    '130808371', #AMERCAN DAIRY SCIENCE ASSOCIATION
]

def process_row(row, *_):
    if not row['supplier_name_fingerprint']:
        return None

    #filter people whose name is all in english. These are not individuals but organizations
    if re.match(r'[a-zA-Z0-9&._ ]+', row['supplier_name_fingerprint']):
        return None

    #If the Israeli ID is too small these are probably not individuals
    company_id = row['company_id'] or ''
    if len(company_id) < 6:
        return None

    if company_id in filter_company_ids:
        return None

    if not is_valid_israeli_id(company_id):
        return None
    return row



process(process_row=process_row)

