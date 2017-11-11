from fuzzywuzzy import process,fuzz

from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, resource_iterator = ingest()

criteria = []
cache = {}


def criteria_scorer(supp, crit):
    return sum(fuzz.UWRatio(supp[k], crit[k])
               for k in ('purpose', 'office'))


def id(x):
    return x

def enrich_supports(rows):
    for row in rows:
        payments = row['payments']
        if payments and len(payments) > 0:
            payment = payments[0]
            key = (payment['support_title'], payment['supporting_ministry'])
            if key in cache:
                bests = cache[key]
            else:
                bests = process.extractBests(
                    {
                        'purpose': key[0],
                        'office': key[1]
                    },
                    criteria,
                    processor=id,
                    scorer=criteria_scorer
                )
                cache[key] = bests
        else:
            bests = []
        row['criteria_docs'] = [x[0] for x in bests]
        yield row


def process_resources(res_iter):
    for res in res_iter:
        if res.spec['name'] == 'criteria':
            criteria.extend(list(res))
            for c in criteria:
                if c['date']:
                    c['date'] = c['date'].isoformat()
        elif res.spec['name'] == 'supports':
            yield enrich_supports(res)
        else:
            yield res


def process_datapackage(dp):
    criteria_res = next(iter(filter(
        lambda r: r['name'] == 'criteria',
        dp['resources']
    )))
    dp['resources'] = list(filter(
        lambda r: r['name'] != 'criteria',
        dp['resources']
    ))
    dp['resources'][-1]['schema']['fields'].append({
        'name': 'criteria_docs',
        'type': 'array',
        'es:itemType': 'object',
        'es:schema': criteria_res['schema']
    })
    return dp


spew(process_datapackage(datapackage),
     process_resources(resource_iterator))