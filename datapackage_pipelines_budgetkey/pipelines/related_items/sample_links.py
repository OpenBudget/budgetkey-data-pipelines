"""Draws a stratified random sample of links for hand-checking precision, and scores checked samples.

  python -m ...related_items.sample_links draw <related_items.csv>... --per-kind 40 --out sample.csv
  python -m ...related_items.sample_links score sample.csv

`draw` writes the sample with an empty `verdict` column. Fill it with y (correct), n (wrong) or ? (can't tell).
Within each target kind the sample is spread over methods. `score` prints precision per kind and per method, with a
95% Wilson interval.
"""
import argparse
import csv
import math
import random
from collections import defaultdict

FIELDS = ['target_kind', 'method', 'relation', 'source_doc_id', 'source_part_id', 'source_title', 'mention',
          'target_doc_id', 'target_title', 'target_year', 'confidence', 'excerpt', 'verdict', 'note']


def wilson(k, n, z=1.96):
    if n == 0:
        return 0, 0
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    r = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (c - r) / d, (c + r) / d


def method_group(method):
    return method.split(':')[0] + (':' + method.split(':')[1] if method.startswith('regex:') else '')


def draw(paths, per_kind, out, seed):
    csv.field_size_limit(1 << 30)
    rows = []
    for path in paths:
        rows.extend(csv.DictReader(open(path, encoding='utf8')))
    rng = random.Random(seed)
    by_kind = defaultdict(lambda: defaultdict(list))
    # One row per (source document, target): the same link repeated for each finding it appears in is one
    # judgement.
    seen = set()
    for r in rows:
        if (r['source_doc_id'], r['target_doc_id']) in seen:
            continue
        seen.add((r['source_doc_id'], r['target_doc_id']))
        by_kind[r['target_kind']][method_group(r['method'])].append(r)
    sample = []
    for kind, methods in sorted(by_kind.items()):
        # Round-robin over methods, so rare methods are checked too.
        pools = {m: rng.sample(rs, len(rs)) for m, rs in methods.items()}
        picked = []
        while len(picked) < per_kind and any(pools.values()):
            for m in sorted(pools):
                if pools[m] and len(picked) < per_kind:
                    picked.append(pools[m].pop())
        sample.extend(picked)
    with open(out, 'w', encoding='utf8', newline='') as f:
        w = csv.DictWriter(f, FIELDS, extrasaction='ignore')
        w.writeheader()
        for r in sample:
            w.writerow(dict(r, verdict='', note=''))
    print('wrote', len(sample), 'links to', out)


def score(path):
    rows = list(csv.DictReader(open(path, encoding='utf8')))
    for key in ('target_kind', 'method'):
        groups = defaultdict(list)
        for r in rows:
            groups[method_group(r[key]) if key == 'method' else r[key]].append(r['verdict'].strip().lower())
        print('\nby', key)
        for g, verdicts in sorted(groups.items()):
            k, n = verdicts.count('y'), verdicts.count('y') + verdicts.count('n')
            lo, hi = wilson(k, n)
            print('  {:28} {:3}/{:3} = {:5.1%}  [{:.0%}-{:.0%}]  ({} unclear)'.format(
                g, k, n, k / n if n else 0, lo, hi, verdicts.count('?')))
    verdicts = [r['verdict'].strip().lower() for r in rows]
    k, n = verdicts.count('y'), verdicts.count('y') + verdicts.count('n')
    print('\nall: {}/{} = {:.1%} [{:.0%}-{:.0%}]'.format(k, n, k / n if n else 0, *wilson(k, n)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='cmd')
    d = sub.add_parser('draw')
    d.add_argument('paths', nargs='+')
    d.add_argument('--per-kind', type=int, default=40)
    d.add_argument('--out', required=True)
    d.add_argument('--seed', type=int, default=1)
    s = sub.add_parser('score')
    s.add_argument('path')
    args = parser.parse_args()
    if args.cmd == 'draw':
        draw(args.paths, args.per_kind, args.out, args.seed)
    else:
        score(args.path)
