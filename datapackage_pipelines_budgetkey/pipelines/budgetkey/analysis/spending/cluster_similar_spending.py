from collections import Counter
from fuzzywuzzy import fuzz
import itertools
import re
from math import log
import logging

from datapackage_pipelines.wrapper import process

NUM_TAGS = 4


def cluster_terms(terms):
    cterms = [[t] for t in terms]
    clusters = []
    while len(cterms)>0:
        added = False
        print(len(cterms))
        cl1 = cterms.pop()
        for cl2 in cterms:
            if any(fuzz.partial_ratio(x, y) >= 90  for x in cl1 for y in cl2):
                cterms.remove(cl2)
                cl1.extend(cl2)
                added = True
        if not added:
            clusters.append(tuple(cl1))
        else:
            cterms.append(cl1)
    assert len(cterms) == 0, 'Len of cterms==%d' % len(cterms)
    ret = []
    for cl in clusters:
        rep = max((len(x), x) for x in cl)[1]
        rep = rep.split()
        final = []
        for word in rep:
            matches = len([x for x in cl if fuzz.partial_ratio(word, x) >= 90])
            if matches > len(cl)/2:
                final.append(word)
        rep = ' '.join(final)
        ret.append((rep, cl))
    return ret


def cluster(items):
    items = [
        item for item in items
        if item['title'] and item['amount']
    ]
    title_clusters = cluster_terms(list(set([
        x['title'] for x in items if x['title']
    ])))
    clusters = [
        dict(
            tag=rep,
            titles=set(titles),
            spending_types=set(),
            amounts=[],
            amount=0,
            count=0,
        )
        for rep, titles in title_clusters
    ]
    for item in items:
        for cluster in clusters:
            if item['title'] in cluster['titles']:
                cluster['amount'] += item['amount']
                cluster['amounts'].append(item['amount'])
                cluster['count'] += 1
                cluster['spending_types'].add(item['spending_type'])
                break
    clusters = sorted(clusters, key=lambda x:-x['amount'])
    if len(clusters) > NUM_TAGS:
        others = clusters[NUM_TAGS:]
        others = dict(
            tag='אחרים',
            titles=None,
            spending_types=set(x for c in others for x in c['spending_types']),
            amounts=[sum(x['amount'] for x in others)],
            amount=sum(x['amount'] for x in others),
            count=sum(x['count'] for x in others),
        )
        clusters = cluster[:NUM_TAGS] + [others]
    for c in clusters:
        del c['titles']
        c['amounts'] = list(reversed(sorted(c['amounts'])))
        c['spending_types'] = sorted(c['spending_types'])
    return clusters


def process_row(row, *_):
    row['spending'] = cluster(row['spending'])
    return row

def modify_datapackage(dp, *_):
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
