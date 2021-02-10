import re

from datapackage_pipelines.wrapper import spew, ingest
from datapackage_pipelines.generators import slugify

parameters, datapackage, res_iter = ingest()

FIELD_VALUES = {
    'direction': [
        'הוצאה',
        'הכנסה',
    ],
    'econ_cls_title_1': [
        '*** ביצוע ללא מקביל בתקציב ***',
        'הוצאות הון',
        'החזר חוב - קרן',
        'החזר חוב - ריבית',
        'הכנסות המדינה - בנק ישראל',
        'הכנסות המדינה - הכנסות',
        'הכנסות המדינה - מילוות',
        'הכנסות המדינה - מענקים',
        'הכנסות מיועדות',
        'העברות',
        'העברות פנים תקציביות',
        'השקעה',
        'חשבונות מעבר',
        'מתן אשראי',
        'קניות',
        'רזרבות',
        'שכר',
    ],
}

HEBREW_WORD = re.compile('[א-ת]+')

slugs = {}

SLUG_LOOKUP = {
    'direction_הוצאה': 'total_direction_expense',
    'direction_הכנסה': 'total_direction_income',
    'econ_cls_title_1_הוצאות הון': 'total_econ_cls_capital_expenditure',
    'econ_cls_title_1_החזר חוב - קרן': 'total_econ_cls_debt_repayment_principal',
    'econ_cls_title_1_החזר חוב - ריבית': 'total_econ_cls_debt_repayment_interest',
    'econ_cls_title_1_הכנסות המדינה - בנק ישראל': 'total_econ_cls_income_bank_of_israel',
    'econ_cls_title_1_הכנסות המדינה - הכנסות': 'total_econ_cls_income',
    'econ_cls_title_1_הכנסות המדינה - מילוות': 'total_econ_cls_income_loans',
    'econ_cls_title_1_הכנסות המדינה - מענקים': 'total_econ_cls_income_grants',
    'econ_cls_title_1_הכנסות מיועדות': 'total_econ_cls_dedicated_income',
    'econ_cls_title_1_העברות': 'total_econ_cls_transfers',
    'econ_cls_title_1_העברות פנים תקציביות': 'total_econ_cls_internal_transfers',
    'econ_cls_title_1_השקעה': 'total_econ_cls_investment',
    'econ_cls_title_1_חשבונות מעבר': 'total_econ_cls_interim_accounts',
    'econ_cls_title_1_מתן אשראי': 'total_econ_cls_credit',
    'econ_cls_title_1_קניות': 'total_econ_cls_procurement',
    'econ_cls_title_1_רזרבות': 'total_econ_cls_reserve',
    'econ_cls_title_1_שכר': 'total_econ_cls_salaries',
    'econ_cls_title_1_*** ביצוע ללא מקביל בתקציב ***': 'total_econ_cls_unknown'
}

def to_slug(k, v):
    slug = k + '_' + v
    return SLUG_LOOKUP[slug]


datapackage['resources'][0]['schema']['fields'].extend(
    {
        'name': to_slug(k, v),
        'type': 'number'
    }
    for k, values in FIELD_VALUES.items()
    for v in values
)


def process_first(rows):
    for row in rows:
        for k in FIELD_VALUES.keys():
            row[to_slug(k, row[k].replace('  ', ' '))] = row.get('net_revised', 0)
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_first(first)

    for res in res_iter_:
        yield res

spew(datapackage, process_resources(res_iter))
