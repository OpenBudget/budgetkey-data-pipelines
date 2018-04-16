from collections import Counter
from Levenshtein import distance
import itertools
import re
from math import log

from datapackage_pipelines.wrapper import process

WORDS = re.compile('[א-ת]+')

def normalize(s):
    return ' '.join(WORDS.findall(s))


def get_terms(s):
    s = normalize(s)
    s = s.split()
    assert len(s) > 0
    n = len(s)
    terms = [] if len(s) > 1 else [tuple([x]) for x in s]
    for j in range(2, n+1):
        subs =[s[i:-j+i+1] for i in range(j-1)]
        subs.append(s[j-1:])
        terms.extend(zip(*subs))
    terms = [' '.join(x) for x in terms]
    return terms


def best_terms(items):
    term_stats = Counter()
    for item in items:
        term_stats.update(dict(
            (k, item['amount'])
            for k in set(get_terms(item['title']))
        ))
    term_stats = [(x, y*log(len(x))) for x,y in term_stats.most_common()]
    term_stats = [(x[0], 
                   sum(y[1] for y in term_stats
                       if distance(y[0], x[0]) <= 1)
                  ) for x in term_stats]
    term_stats = sorted(term_stats, key=lambda x: -x[1])
    return [ts[0] for ts in term_stats if 
            distance(term_stats[0][0], ts[0]) <= 1]


def group_same_items(items):
    titles = {}
    for item in items:
        titles.setdefault((item['title'], item['spending_type']), []).append(item)
    ret = []
    for (t, st), itms in titles.items():
        ret.append(dict(
            title=t,
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
    for i in items:
        i['title'] = normalize(i['title'])
    items = [
        i for i in items 
        if i['title']
    ]
    while len(items) > 0:
        terms = best_terms(items)
        term_items = []
        ret[terms[0]] = term_items
        new_items = []
        for item in items:
            added = False
            for term in terms:
                if term in item['title']:
                    term_items.append(item)
                    added = True
                    break
            if not added:
                new_items.append(item)
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
    if len(ret) > NUM_TAGS:
        rest = ret[NUM_TAGS:]
        ret = ret[:NUM_TAGS]
        ret.append(
            dict(
                tag='אחרים',
                amount=sum(i['amount'] for x in rest for i in x['items']),
                spending_types=sorted(set(i['spending_type'] for x in rest for i in x['items'])),
                items=list(itertools.chain(*(i['items'] for i in rest))),                
            )
        )
    return ret


def process_row(row, *_):
    row['spending'] = cluster(row['spending'])
    return row

def modify_datapackage(dp, *_):
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
