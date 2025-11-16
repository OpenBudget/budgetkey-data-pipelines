import dataflows as DF
import re
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id

URL = 'https://pob.education.gov.il/umbraco/api/content/GetKolKore?a=1&israshut=0&rdo=0&length=0&page=0&pagesize=20&id=0'
PAGE_URL = 'https://pob.education.gov.il/kolotkorim/kolkore/'

HE = re.compile('[א-ת]+')
EMAIL = re.compile('[a-zA-Z0-9_.]+@[a-zA-Z0-9_.]+')
NUMBERS = re.compile('[0-9]+')


def extract_hebrew(row, field):
    if row.get(field):
        return ' '.join(HE.findall(row[field]))


def extract_email(row, field):
    if row.get(field):
        return ''.join(EMAIL.findall(row[field])[:1])


def extract_budget_code(row, field):
    if row.get(field):
        codes = [c for c in row[field].split(',') if 'מרכז קרנות' not in c]
        if len(codes) != 1:
            # Something is wrong
            return None
        code = ''.join(NUMBERS.findall(codes[0]))
        if len(code) == 6:
            code = '20' + code
        if len(code) == 8:
            code = '00' + code
        if len(code) == 10:
            return code
    


def enumerate_titles(rows):
    if rows.res.name == 'education':
        titles = set()
        for row in rows:
            orig_title = row['page_title']
            title = orig_title
            count = 2
            while title in titles:
                title = f'{orig_title} ({count})'
                count += 1
            titles.add(title)
            row['page_title'] = title
            yield row
        else:
            yield from rows
        

def flow(*_):
    return DF.Flow(
        DF.load(URL, format='json', name='education'),
        # DF.checkpoint('education'),
        DF.concatenate(dict(
            page_title=['name'],
            start_date=['taarichPirsum'],
            claim_date=['taarichLast'],
            target_audience_x=['gufim'],
            description=['summary'],
            email=['ishKesherPedagogi'],
            publishing_unit_x=['yechida'],
            budget_code_x=['takana'],
            # att_title=['name'],
            att_url=['linkCriteria'],
            publication_id=['id']
        ), resources=-1, target=dict(name='education')),
        enumerate_titles,
        DF.add_field('page_url', 'string', PAGE_URL, resources=-1),
        DF.add_field('publisher', 'string', 'משרד החינוך', resources=-1),
        DF.add_field('tender_type', 'string', 'call_for_bids', resources=-1),
        DF.add_field('tender_type_he', 'string', 'קול קורא', resources=-1),
        # DF.add_field('publication_id', 'integer', 0, resources=-1),
        DF.add_field('tender_id', 'string', '0', resources=-1),
        DF.add_field('tender_type_he', 'string', 'קול קורא', resources=-1),
        DF.add_field('contact', 'string', lambda row: extract_hebrew(row, 'email'), resources=-1),
        DF.add_field('target_audience', 'string', lambda row: extract_hebrew(row, 'target_audience_x'), resources=-1),
        DF.add_field('contact_email', 'string', lambda row: extract_email(row, 'email'), resources=-1),
        DF.add_field('publishing_unit', 'string', lambda row: row['publishing_unit_x'], resources=-1),
        DF.add_field('budget_code', 'string', lambda row: extract_budget_code(row, 'budget_code_x'), resources=-1),
        DF.set_type('start_date', type='date', format='%d-%m-%Y'),
        DF.set_type('claim_date', type='date', format='%d-%m-%Y'),
        DF.add_field('documents', 'array',
                     lambda row: [dict(
                         description=row['page_title'],
                         link=row['att_url'],
                         update_time=str(row['start_date'])
                     )], resources=-1),
        DF.delete_fields(['email', 'publishing_unit_x', 'budget_code_x', 'att_url', 'target_audience_x'], resources=-1),
        calculate_publication_id(6),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )


if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer(max_cell_size=40, num_rows=10000),
    ).process()
