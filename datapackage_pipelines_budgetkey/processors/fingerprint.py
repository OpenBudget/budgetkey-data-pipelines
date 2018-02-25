import re

from datapackage_pipelines.wrapper import ingest, spew

DIGITS = re.compile('\d+')
ENGLISH = re.compile('[a-zA-Z]+')

CLEAN_WORDS = [ 'בע"מ',
                'בעמ',
                "בע'מ",
                '(חל"צ)',
                'חברה לתועלת הציבור',
                'בע"מ.',
                'אינק.',
                'לימיטד',
                'בע"מ"',
                'בע,מ',
                'עב"מ',
                'בע"ם',
                '(ע"ר)',
                "(ע''ר)",
                'בע"מ (חל"צ)',
                '(ער)',
                '{ער}',
                '}ער{',
                '(חלצ)',
                'ישראל'
                ]

CLEAN_TITLES = [ 'עירית',
                 'עיריית',
                 'מ.א.',
                 "מ.מ.",
                 'מ.א',
                 "מ.מ",
                 "מ. מ.",
                 "מ. א.",
                 "מועצה איזורית",
                 "מועצה אזורית",
                 "מועצה מקומית",
                 'מוא"ז',
                 'עמותת',
                 ]

SIMPLIFICATIONS = [
                    ('*', ''),
                    (" אל-", " "),
                    ('"', ''),
                    ("'", ""),
                    ("-", " "),
                    ("וו", "ו"),
                    ("יי", "י"),
                    (",", ""),
                    (". ", " "),
                    (".", " "),
                    (")", " "),
                    ("(", " "),
                    ("/", " "),
                    ("\\", " "),
                 ]


CLEAN_SUFFIXES = set()
for w in CLEAN_WORDS:
    for i in range(1, len(w)+1):
        CLEAN_SUFFIXES.add(" "+w[:i])


def fingerprint(rows, src_field, tgt_field, unique_fingerprints):
    used = set()
    for row in rows:
        name = row[src_field]
        tgt = None
        if name is not None:
            tgt = name.strip().lower()

            done = False
            while not done:
                done = True
                for suffix in CLEAN_SUFFIXES:
                    for l in range(len(suffix), 0, -1):
                        if tgt.endswith(suffix[:l]):
                            tgt = tgt[:-l]
                            done = False
                            break

            for prefix in CLEAN_TITLES:
                if tgt.startswith(prefix + ' '):
                    tgt = tgt[len(prefix)+1:]

            for f, t in SIMPLIFICATIONS:
                tgt = tgt.replace(f, t)

            tgt_with_digits = tgt.strip()
            tgt_no_digits = DIGITS.sub('', tgt_with_digits).strip()
            tgt_no_english = ENGLISH.sub('', tgt_no_digits).strip()

            tgt = tgt_no_english
            if len(tgt) == 0:
                tgt = tgt_no_digits
                if len(tgt) == 0:
                    tgt = tgt_with_digits
                    if len(tgt) == 0:
                        tgt = None
            
            if tgt is not None:
                tgt = ' '.join(sorted(tgt[:30].split()))

        if not unique_fingerprints or tgt not in used:
            row[tgt_field] = tgt
            yield row
            used.add(tgt)


def process_resources(res_iter_, src_field, tgt_field, unique_fingerprints):
    for res in res_iter_:
        if res.spec['name'] == res_name:
            yield fingerprint(res, src_field, tgt_field, unique_fingerprints)
        else:
            yield res


if __name__ == "__main__":
    parameters, dp, res_iter = ingest()
    src_field = parameters['source-field']
    tgt_field = parameters['target-field']
    res_name = parameters['resource-name']
    unique_fingerprints = parameters.get('unique-fingerprints', False)

    resource = next(iter(filter(
        lambda res: res['name'] == res_name,
        dp['resources']
    )))
    resource['schema']['fields'].append({
        'name': tgt_field,
        'type': 'string'
    })

    spew(dp, process_resources(res_iter, src_field, tgt_field, unique_fingerprints))