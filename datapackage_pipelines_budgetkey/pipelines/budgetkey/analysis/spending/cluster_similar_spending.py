from collections import Counter
from Levenshtein import distance
import itertools

from datapackage_pipelines.wrapper import process

def get_terms(s):
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
    term_stats = [(x, y*len(x)) for x,y in term_stats.most_common()]
    term_stats = sorted(term_stats, key=lambda x: -x[1])
    return [ts[0] for ts in term_stats if 
            distance(term_stats[0][0], ts[0]) <= 1]


def cluster(items):
    ret = {}
    items = [
        i for i in items 
        if i['amount']
    ]
    for i in items:
        i['title'] = ' '.join(i['title'].split())
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
            items=items,
        ) 
        for tag, items in ret.items()
    ]
    ret = sorted(ret, key=lambda x: -x['amount'])
    if len(ret) > 20:
        rest = ret[20:]
        ret = ret[:20]
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
