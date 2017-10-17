import re

from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()
src_field = parameters['source-field']
tgt_field = parameters['target-field']
res_name = parameters['resource-name']

DIGITS = re.compile('\d+')
ENGLISH = re.compile('[a-zA-Z]+')

CLEAN_WORDS = [ 'בע"מ',
                'בעמ',
                "בע'מ",
                '(חל"צ)',
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


def fingerprint(rows):
    for row in rows:
        name = row[src_field]
        tgt = None
        if name is not None:
            tgt = name.strip()

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

            tgt = DIGITS.sub('', tgt)
            tgt = ENGLISH.sub('', tgt)

            tgt = ' '.join(sorted(tgt.strip()[:30].split()))

            if len(tgt) == 0:
                tgt = None

        row[tgt_field] = tgt
        yield row


def process_resources(res_iter_):
    for res in res_iter_:
        if res.spec['name'] == res_name:
            yield fingerprint(res)
        else:
            yield res

import logging; logging.info(res_name)
resource = next(iter(filter(
    lambda res: res['name'] == res_name,
    dp['resources']
)))
resource['schema']['fields'].append({
    'name': tgt_field,
    'type': 'string'
})

spew(dp, process_resources(res_iter))