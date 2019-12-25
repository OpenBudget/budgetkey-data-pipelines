from hashlib import md5
import json


def calculate_publication_id(factor):
    def func(row):
        if not row.get('publication_id'):
            title_hash = int.from_bytes(
                md5((
                    str(row['publisher']) +
                    str(row['page_title']) +
                    str(row['start_date'])
                ).encode('utf8')).digest()[:4], 'big'
            )
            mod = 100000000
            title_hash = factor*mod + (title_hash % mod)
            row['publication_id'] = title_hash
    return func
