# Ministry of Education - Kindergartens
# https://parents.education.gov.il/prhnet/gov-education/kindergarten/search-daycare
# https://parents.education.gov.il/api/data/meonot/GetMeonotExcel?year=2027&shemYeshuv=&shemRechov=&misparBait=&radius=0

from datetime import datetime
import dataflows as DF


# Year in 4 months from now
YEAR = datetime.now().year + (1 if datetime.now().month >= 9 else 0)


def hebrew_year(year):
    # Gregorian year in which the school year ends -> Hebrew year, e.g. 2027 -> תשפ״ז
    n = (year + 3760) % 1000
    letters = ''
    for value, letter in [(400, 'ת'), (300, 'ש'), (200, 'ר'), (100, 'ק')]:
        while n >= value:
            letters += letter
            n -= value
    if n in (15, 16):
        letters += 'ט' + ('ו' if n == 15 else 'ז')
    else:
        letters += ' יכלמנסעפצ'[n // 10].strip() + ' אבגדהוזחט'[n % 10].strip()
    if len(letters) == 1:
        return letters + '׳'
    return letters[:-1] + '״' + letters[-1]


HEB_YEAR = hebrew_year(YEAR)

# url = f'https://parents.education.gov.il/prhnet/Api/MeonotController/GetExcel?0={YEAR}&1=0&2=0&3=0&4=0&5=0&csrt=4274816473439949448'
url = f'https://parents.education.gov.il/api/data/meonot/GetMeonotExcel?year={YEAR}&shemYeshuv=&shemRechov=&misparBait=&radius=0'

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Content-Type': 'application/json',
    # 'Request-Id': '|imlaC.oxafW',
    # 'X-TS-AJAX-Request': 'true',
    # 'X-Security-CSRF-Token': '08a139caf1ab2800566c84b0c629a5432d57d519dc9eb1cde690348ead583e4305302c9484428e04c2acc71673da6d05',
    'Alt-Used': 'parents.education.gov.il',
    'Connection': 'keep-alive',
    'Referer': 'https://parents.education.gov.il/gov-education/daycare-home/search-daycare',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=0'
}

def join_address(address, city):
    if city not in address:
        return f'{address}, {city}'
    return address

def scrape():
    return DF.Flow(
        DF.load(url, format='xlsx', http_headers=headers),
        DF.checkpoint('moe'),
        DF.add_field('moe_name', 'string', lambda r: r['שם וסמל מעון'].rsplit('-', 1)[0].strip()),
        DF.add_field('moe_school_year', 'string', HEB_YEAR),
        DF.add_field('moe_symbol', 'string', lambda r: r['שם וסמל מעון'].rsplit('-', 1)[1].strip()),
        DF.add_field('moe_mol_symbol', 'string', lambda r: f'mol-' + str(r['זרוע העבודה']).strip() if r['זרוע העבודה'] else None),
        DF.add_field('moe_owner', 'string', lambda r: (r['בעלות'] or '').strip()),
        DF.add_field('moe_sector', 'string', lambda r: r['מגזר']),
        DF.add_field('moe_license_status', 'string', lambda r: r['סטטוס רישוי']),
        DF.add_field('moe_city', 'string', lambda r: r['יישוב']),
        DF.add_field('moe_address', 'string', lambda r: join_address(r['כתובת'], r['moe_city'])),
        DF.add_field('moe_phone', 'string', lambda r: r['טלפון']),
        DF.add_field('moe_manager_name', 'string', lambda r: (r['מנהל/ת'] or '').strip()),
        DF.filter_rows(lambda r: r['moe_license_status'] != 'מעון סגור'),
        DF.filter_rows(lambda r: bool(r['moe_symbol'])),
        DF.add_field('_id', 'string', lambda r: f'moe-{r["moe_symbol"]}'),
        DF.select_fields(['_id', 'moe_.+']),
        DF.add_field('source', 'string', 'moe'),
        DF.update_resource(-1, name='moe', path='moe.csv'),
        DF.printer()
    )

def flow(*_):
    return DF.Flow(
        scrape(),
        DF.dump_to_path('/var/datapackages/facilities/education'),
        DF.dump_to_sql(dict(
            facilities_education={'resource-name': 'moe'}
        )),
        DF.dump_to_sql(dict([('facilities_education_%s' % YEAR, {'resource-name': 'moe'})])),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )

if __name__ == '__main__':
    scrape().process()