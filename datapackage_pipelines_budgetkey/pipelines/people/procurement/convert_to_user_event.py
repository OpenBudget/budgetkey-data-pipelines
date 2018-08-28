from datapackage_pipelines.wrapper import process
import logging


def process_row(row, *_):
    full_name = row['supplier_name'][-1] if row['supplier_name'] else None
    publisher_name = row['publisher_name'] or row['publisher']
    amount = row['executed']
    purpose = row['purpose']
    details = '{full_name} קיבל/ה {amount:,} מ{publisher_name} עבור {purpose}'.format(**{
        'full_name':full_name,
        'amount':amount,
        'publisher_name': publisher_name,
        'purpose': purpose
    })
    proof_url = row['payments'][-1]['url']
    return {'event': 'procurement',
            'when': row['order_date'],
            'doc_id': None,
            'full_name': full_name,
            'company': None,
            'title': details,
            'sources': [
                {
                    'full_name':full_name,
                    'details': details,
                    'proof_url':proof_url,
                    'amount': amount,
                    'publisher_name': publisher_name,
                    'purpose': purpose,
                    'company':None
                }
            ],}


def modify_datapackage(dp, *_):
     dp['resources'][0]['schema']['fields'] = [
         {'name': 'event', 'type': 'string'},
         {'name': 'when', 'type': 'date', 'format':'%Y-%m-%d'},
         {'name': 'doc_id', 'type': 'string'},
         {'name': 'full_name', 'type': 'string'},
         {'name': 'company', 'type': 'string'},
         {'name': 'title', 'type': 'string'},
         {'name': 'sources', 'type': 'array'},
     ]
     return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
