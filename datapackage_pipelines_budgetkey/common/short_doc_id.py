import hashlib

def calc_short_doc_id(doc_id):
    hashed_id = hashlib.md5(doc_id.encode('utf8')).hexdigest()[:12]
    return 'https://next.obudget.org/i/' + hashed_id