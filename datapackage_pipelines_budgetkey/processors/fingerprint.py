import re
import os
import logging

from sqlalchemy import create_engine

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
                    ('%', ''),
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

def calc_fingerprint(name):
    tgt = name
    options = []
    if tgt is not None:
        tgt = name.strip().lower()
        options.append(tgt)

        done = False
        while not done:
            done = True
            for suffix in CLEAN_SUFFIXES:
                for l in range(len(suffix), 0, -1):
                    if tgt.endswith(suffix[:l]):
                        tgt = tgt[:-l]
                        options.append(tgt)
                        done = False
                        break

        for prefix in CLEAN_TITLES:
            if tgt.startswith(prefix + ' '):
                tgt = tgt[len(prefix)+1:]
                options.append(tgt)

        for f, t in SIMPLIFICATIONS:
            tgt = tgt.replace(f, t)

        options.append(tgt)

        tgt = DIGITS.sub('', tgt)
        options.append(tgt)

        tgt = ENGLISH.sub('', tgt)
        options.append(tgt)

        if not tgt:
            tgt = options[0]
            for opt in reversed(options):
                if opt:
                    tgt = opt.strip()
                    break
        
        if tgt is not None:
            tgt = ' '.join(sorted(tgt[:30].split()))

    if not tgt:
        tgt = '<empty>'
        
    return tgt

ids = {}
fps = {}


def fingerprint(rows, src_field, tgt_field, src_id_field, unique_fingerprints):
    used = set()
    engine = None
    conn = None
    for row in rows:
        name = row[src_field]
        if tgt_field:
            tgt = calc_fingerprint(name)
            if not unique_fingerprints or tgt not in used:
                row[tgt_field] = tgt
                yield row
                if unique_fingerprints:
                    used.add(tgt)
        else:
            if conn is None:
                engine = create_engine(os.environ['DPP_DB_ENGINE'])
                conn = engine.connect()
            query = "select id, name, kind from entity_fingerprints where {}='{}' limit 1"
            res = None
            results = []
            if src_id_field:
                id = row[src_id_field]
                if id:
                    if id in ids:
                        res = ids[id]
                    else:
                        results = conn.execute(query.format('id', row[src_id_field])).fetchall()
                        if len(results) > 0:
                            res = tuple(results[0])
                            ids[id] = res
            if len(results) == 0:
                fp = calc_fingerprint(row[src_field])
                if fp in fps:
                    res = fps[fp]
                else:
                    results = conn.execute(query.format('fingerprint', fp)).fetchall()
                    if len(results) > 0:
                        res = tuple(results[0])
                        fps[fp] = res
            if res is not None:
                row['entity_id'], row['entity_name'], row['entity_kind'] = res[0], res[1], res[2]
            else:
                row['entity_id'], row['entity_name'], row['entity_kind'] = None, None, None
            yield row


def process_resources(res_iter_, src_field, tgt_field, src_id_field, unique_fingerprints):
    for res in res_iter_:
        if res.spec['name'] == res_name:
            yield fingerprint(res, src_field, tgt_field, src_id_field, unique_fingerprints)
        else:
            yield res


if __name__ == "__main__":
    parameters, dp, res_iter = ingest()
    src_field = parameters['source-field']
    src_id_field = parameters.get('source-id-field')
    tgt_field = parameters.get('target-field')

    res_name = parameters['resource-name']
    unique_fingerprints = parameters.get('unique-fingerprints', False)

    resource = next(iter(filter(
        lambda res: res['name'] == res_name,
        dp['resources']
    )))
    if tgt_field:
        resource['schema']['fields'].append({
            'name': tgt_field,
            'type': 'string'
        })
    else:
        resource['schema']['fields'].extend([{
            'name': 'entity_id',
            'type': 'string'
        }, {
            'name': 'entity_name',
            'type': 'string'
        }, {
            'name': 'entity_kind',
            'type': 'string'
        }])
        
    spew(dp, process_resources(res_iter, src_field, tgt_field, src_id_field, unique_fingerprints))
