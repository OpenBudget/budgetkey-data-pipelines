import re

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()


KINDS = {'conurbation': 'איגוד ערים',
         'drainage_authority': 'רשות ניקוז',
         'foreign_company': 'חברת חוץ',
         'foreign_representative': 'נציגות זרה',
         'gamach': 'גמילות חסדים',
         'government_office': 'משרד ממשלתי',
         'health_service': 'שירות בריאות',
         'house_committee': 'ועד בית',
         'law_mandated_organization': 'תאגיד סטטוטורי',
         'local_community_committee': 'ועד מקומי בישוב',
         'local_planning_committee': 'ועדה מקומית לתכנון',
         'municipal_parties': 'רשימה לרשות מקומית',
         'municipal_precinct': 'רובע עירוני',
         'municipality': 'רשות מקומית',
         'professional_association': 'איגוד מקצועי',
         'provident_fund': 'קופת גמל',
         'religion_service': 'שירות דת',
         'religious_court_sacred_property': 'הקדש בית דין דתי',
         'west_bank_corporation': "תאגיד יו״ש"}


def process_resource(res_):
    for row in res_:
        row['kind_he'] = KINDS[row['kind']]
        yield row


def process_resources(res_iter_):
    for res in res_iter_:
        yield process_resource(res)

datapackage['resources'][0]['schema']['fields'].append({
    'name': 'kind_he',
    'type': 'string'
})

spew(datapackage, process_resources(res_iter))