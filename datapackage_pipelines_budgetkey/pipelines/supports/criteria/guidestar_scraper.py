import datetime
import dataflows as DF
from datapackage_pipelines_budgetkey.common.guidestar_api import GuidestarApi


def scrape():
    base_url = 'https://www.guidestar.org.il/test-support'
    api = GuidestarApi(base_url)
    data = api.prepare().method('getAllTestSupports', None, 43).run()
    data = data[0]['result']
    for item in data:
        try:
            publisher = item['OrganType']
            budget_code = item.get('Budget')
            if budget_code:
                budget_codes = budget_code\
                    .replace('-', '')\
                    .replace('.', '')\
                    .replace(',', ';')\
                    .replace('ו', ';')\
                    .split(';')
                cleaned_budget_codes = []
                for budget_code in budget_codes:
                    budget_code = budget_code.strip()
                    if len(budget_code) == 6 and publisher == 'משרד החינוך':
                        budget_code = '20' + budget_code
                    if len(budget_code) == 6 and publisher == 'משרד התרבות והספורט':
                        budget_code = '19' + budget_code
                    if len(budget_code) == 7 and budget_code[0] == '4' and publisher in (
                        'המשרד לפיתוח הפריפריה, הנגב והגליל',
                        'המשרד לשוויון חברתי',
                        'המשרד לשיתוף פעולה אזורי',
                        'המשרד לשירותי דת',
                    ):
                        budget_code = '0' + budget_code
                    if len(budget_code) == 7 and budget_code[0] == '8' and publisher in (
                        'משרד המשפטים',
                    ):
                        budget_code = '0' + budget_code
                    if len(budget_code) == 8:
                        budget_code = '00' + budget_code
                        cleaned_budget_codes.append(budget_code)
            creation_date = item.get('MaxUpdatedCreatedDate', item.get('CreationDate', item.get('DateUpdated', item.get('StartDate'))))
            if creation_date:
                creation_date = datetime.date.fromtimestamp(creation_date / 1000)
            else:
                creation_date = datetime.date(year=int(item['YearCreated']), month=1, day=1)
            yield dict(
                publication_id=0,
                tender_type='support_criteria',
                tender_type_he='מבחן תמיכה',
                tender_id=item['Id'],
                budget_codes=cleaned_budget_codes,
                start_date=creation_date,
                description=item.get('Description'),
                documents=[dict(link=item['FileLink'], description='מבחן התמיכה')],
                publisher=publisher,
                page_title=item['SupportName'],
                page_url=f'{base_url}/{item["Id"]}',
            )
        except Exception as e:
            print(repr(e), item)


def flow(*_):
    return DF.Flow(
        scrape(),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )


if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer()
    ).process()