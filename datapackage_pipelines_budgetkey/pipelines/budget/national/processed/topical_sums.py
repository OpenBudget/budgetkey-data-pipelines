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
        'העברות  פנים תקציביות',
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


def to_slug(k, v):
    slug = k + '_' + v
    return slug


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
            row[to_slug(k, row[k])] = row.get('net_revised', 0)
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_first(first)

    for res in res_iter_:
        yield res

spew(datapackage, process_resources(res_iter))
