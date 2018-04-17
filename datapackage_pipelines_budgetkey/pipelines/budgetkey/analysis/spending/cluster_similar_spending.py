from collections import Counter
from Levenshtein import distance
import itertools
import re
from math import log
import logging

from datapackage_pipelines.wrapper import process

WORDS = re.compile('[א-ת]+')

def normalize(s):
    for x in """,'".:;()""":
        s = s.replace(x, '')
    return ' '.join(WORDS.findall(s))


def get_terms(s):
    s = normalize(s)
    s = s.split()
    assert len(s) > 0
    n = min([7, len(s)])
    terms = [] if len(s) > 1 else [tuple([x]) for x in s]
    for j in range(2, n+1):
        subs =[s[i:-j+i+1] for i in range(j-1)]
        subs.append(s[j-1:])
        terms.extend(zip(*subs))
    terms = [' '.join(x) for x in terms]
    return terms

def cdistance(s1, s2, cache):
    if s1 in s2: return 0
    key = (s1, s2) if s1 > s2 else (s2, s1)
    if key not in cache:
        cache[key] = distance(s1, s2)
    return cache[key]


def cluster_terms(terms):
    cterms = [[t] for t in terms]
    ret = []
    while len(cterms)>0:
        added = False
        cl1 = cterms.pop()
        for cl2 in cterms:
            if any(distance(x, y) <= 1 for x in cl1 for y in cl2):
                cterms.remove(cl2)
                cterms.append(cl1 + cl2)
                added = True
                break
        if not added:
            ret.append(tuple(cl1))
    assert len(cterms) == 0, 'Len of cterms==%d' % len(cterms)
    return ret


def best_terms(items):
    term_stats = Counter()
    term_counts = Counter()
    all_terms = set()
    for item in items:
        terms = set(get_terms(item['title']))
        all_terms.update(terms)
        term_stats.update(dict(
            (k, item['amount'])
            for k in terms
        ))
        term_counts.update(terms)

    logging.info('all_terms %d', len(all_terms))
    all_terms = cluster_terms(all_terms)
    logging.info('all_terms clusters %d', len(all_terms))
    logging.info('.')

    term_stats = dict(term_stats.most_common())
    term_stats = [
        (k, sum(term_stats[kk] for kk in k)) for k in all_terms
    ]

    term_counts = dict(term_counts.most_common())
    term_counts = [
        (k, sum(term_counts[kk] for kk in k)) for k in all_terms
    ]

    term_counts = dict((k, log(len(items) / v)) for k, v in term_counts)
    term_stats = [(k, v*term_counts[k] + sum(len(x) for x in k)) for k, v in term_stats]

    term_stats = sorted(term_stats, key=lambda x: -x[1])
    term_stats = [x[0] for x in term_stats]
    return term_stats


def group_same_items(items):
    titles = {}
    for item in items:
        titles.setdefault((normalize(item['title']), item['spending_type']), []).append(item)
    ret = []
    for (_, st), itms in titles.items():
        ret.append(dict(
            title=itms[0]['title'],
            amount=sum(i['amount'] for i in itms),
            spending_type=st,
        ))
    return ret

NUM_TAGS = 4


def cluster(items):
    ret = {}
    items = [
        i for i in items 
        if i['amount']
    ]
    items = [
        i for i in items 
        if normalize(i['title'])
    ]
    bterms = best_terms(items)
    while len(items) > 0 and len(ret) < NUM_TAGS:
        terms = bterms.pop(0)
        rep = max(terms)
        term_items = []
        ret[rep] = term_items
        new_items = []
        for item in items:
            added = False
            for term in terms:
                if term in normalize(item['title']):
                    term_items.append(item)
                    added = True
                    break
            if not added:
                new_items.append(item)
        if len(term_items) == 0:
            del ret[rep]
        assert len(new_items) < len(items)
        items = new_items
    ret = [
        dict(
            tag=tag,
            amount=sum(i['amount'] for i in items),
            spending_types=sorted(set(i['spending_type'] for i in items)),
            items=group_same_items(items),
        ) 
        for tag, items in ret.items()
    ]
    ret = sorted(ret, key=lambda x: -x['amount'])
    if len(items) > 0:
        ret.append(
            dict(
                tag='אחרים',
                amount=sum(i['amount'] for i in items),
                spending_types=sorted(set(i['spending_type'] for i in items)),
                items=items,                
            )
        )
    return ret


def process_row(row, *_):
    logging.info('Clustering row %s', ' '.join(str(v) for k, v in row.items() if k != 'spending'))
    logging.info('Num items %s', len(row['spending']))
    logging.info('.')
    row['spending'] = cluster(row['spending'])
    return row

def modify_datapackage(dp, *_):
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
