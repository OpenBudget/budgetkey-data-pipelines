from dataflows import add_computed_field

def verify_row_count(row, expected_fields, expected_count, is_required=True):
    doc = row['document']
    url = row['url']
    for field in expected_fields:
        if not is_required and field not in doc:
            return
        if field not in doc:
            raise Exception("Document {} failed validation. textAlias {} not found. (expected: {})".format(url, field, expected_count))
        elif len(doc[field]) != expected_count:
            raise Exception("Document {} failed validation. textAlias {} found {} times. (expected: {})".format(url, field, len(doc[field]), expected_count))

def verify_same_row_count(row, expected_fields):
    expected_count = max([len(row.get(f, []) or []) for f in expected_fields])
    if expected_count>0:
        verify_row_count(row, expected_fields, expected_count)

def add_fields(names, type):
    return add_computed_field([dict(target=name,
                                    type=type,
                                    operation=(lambda row: None)
                                    ) for name in names])


def verify_row_values(row, field, is_valid):

    if not callable(is_valid):
        values = is_valid
        is_valid = lambda x: x in values

    doc = row['document']
    url = row['url']
    if not all(is_valid(x) for x in doc.get(field,[])):
        raise Exception("Document {} failed validation. textAlias {} had illegal value: {}".format(url, field, doc[field]))

def rename_fields(rename_fields):
    def iter(rows):
        for row in rows:
            doc = row['document']

            for (k, v) in rename_fields.items():
                if k in doc:
                    doc[v] = doc[k]
                    del doc[k]
            yield row
    return iter


def fix_fields(fields):
    def iter(rows):
        for row in rows:
            for f in fields:
                if row[f] is None:
                    row[f] = ""
                if row[f] == ('_' * len(row[f])):
                    row[f] = ""
                if row[f] == ('-' * len(row[f])):
                    row[f] = ""
            yield row
    return iter