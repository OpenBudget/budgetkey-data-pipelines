from hashlib import md5


def calculate_publication_id(factor):
    def func(row):
        title_hash = int.from_bytes(
            md5(
                (str(row['publisher']) + str(row['page_title'])).encode('utf8')
            ).digest()[:4],
            'big'
        )
        mod = 1000000000
        title_hash = factor*mod + (title_hash % mod)
        row['publication_id'] = title_hash
    return func
