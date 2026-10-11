import csv
import os
import re
import logging
from concurrent.futures import ThreadPoolExecutor, wait
from urllib.parse import urlparse, unquote

import requests
import dataflows as DF


HERE = os.path.dirname(os.path.abspath(__file__))
USER_AGENT = 'budgetkey-data-pipelines/1.0 (https://next.obudget.org; municipal websites)'

AUTHORITIES_URL = 'https://data.gov.il/api/3/action/datastore_search'
AUTHORITIES_RESOURCE = 'c4916937-f5d3-4295-a22e-88a1af5cde6a'  # רשימה של הרשויות המוניציפאליות - מאוחד
WIKIDATA_SPARQL = 'https://query.wikidata.org/sparql'
HEWIKI_API = 'https://he.wikipedia.org/w/api.php'
# DNS over HTTPS: municipal name servers often don't answer queries from abroad
DNS_API = 'https://dns.google/resolve'
OFFICIAL_SUFFIXES = ('.muni.il', '.gov.il', '.org.il')

# When several Wikidata items share a CBS code, prefer the one that is a municipality
AUTHORITY_CLASSES = {
    'Q3505887',  # city council
    'Q1288520',  # local council in Israel
    'Q1022469',  # regional council of Israel
    'Q2762194',  # local government in Israel
    'Q515',  # city
    'Q1549591',  # big city
}

INFOBOX_WEBSITE = re.compile(r'^\s*\|\s*(?:אתר אינטרנט|אתר)\s*=\s*(.*)$', re.M)
URL = re.compile(r'https?://[^\s|\]\[}{<>"]+')


def get(url, **kwargs):
    headers = {'User-Agent': USER_AGENT}
    headers.update(kwargs.pop('headers', {}))
    resp = requests.get(url, headers=headers, timeout=60, **kwargs)
    resp.raise_for_status()
    return resp.json()


def sparql(query):
    resp = requests.post(WIKIDATA_SPARQL, data=dict(query=query), timeout=120, headers={
        'User-Agent': USER_AGENT, 'Accept': 'application/sparql-results+json'
    })
    resp.raise_for_status()
    for binding in resp.json()['results']['bindings']:
        yield {k: v['value'].replace('http://www.wikidata.org/entity/', '') for k, v in binding.items()}


def read_csv(name):
    with open(os.path.join(HERE, name)) as f:
        return {row['authority_code']: row for row in csv.DictReader(f)}


def fetch_authorities():
    data = get(AUTHORITIES_URL, params=dict(resource_id=AUTHORITIES_RESOURCE, limit=1000),
               headers={'User-Agent': 'Mozilla/5.0'})
    records = data['result']['records']
    assert len(records) > 200, 'Expected 250+ authorities, got %d' % len(records)
    return records


def fetch_wikidata_items(codes, crosswalk):
    # Settlements by CBS code, plus pinned items from the crosswalk
    code_values = ' '.join('"%s" "%s"' % (c, c.zfill(4)) for c in codes)
    by_code = {}
    for row in sparql('''
        SELECT ?code ?item (GROUP_CONCAT(DISTINCT ?type) AS ?types) WHERE {
          VALUES ?code { %s }
          ?item wdt:P3466 ?code .
          OPTIONAL { ?item wdt:P31 ?type }
        } GROUP BY ?code ?item''' % code_values):
        by_code.setdefault(row['code'].lstrip('0'), []).append(row)

    selected = {}
    for code in codes:
        if code in crosswalk:
            selected[code] = crosswalk[code]['wikidata_id']
            continue
        candidates = by_code.get(code, [])
        if len(candidates) > 1:
            preferred = [c for c in candidates
                         if AUTHORITY_CLASSES & set(t.split('/')[-1] for t in c.get('types', '').split())]
            if len(preferred) == 1:
                candidates = preferred
            else:
                logging.warning('Ambiguous Wikidata items for authority %s: %s', code,
                                [c['item'] for c in candidates])
                continue
        if candidates:
            selected[code] = candidates[0]['item']
    return selected


def fetch_wikidata_details(qids):
    details = {qid: dict(item_websites=set(), org_websites=set(), hewiki=None) for qid in qids}
    values = ' '.join('wd:%s' % qid for qid in qids)
    for row in sparql('''
        SELECT ?item ?website ?article WHERE {
          VALUES ?item { %s }
          OPTIONAL { ?item wdt:P856 ?website }
          OPTIONAL { ?article schema:about ?item ; schema:isPartOf <https://he.wikipedia.org/> }
        }''' % values):
        d = details[row['item']]
        if row.get('website'):
            d['item_websites'].add(row['website'])
        if row.get('article'):
            d['hewiki'] = unquote(row['article'].split('/wiki/', 1)[1]).replace('_', ' ')
    # The municipality as an organization, e.g. Jerusalem -> עיריית ירושלים
    for row in sparql('''
        SELECT ?item ?website WHERE {
          VALUES ?item { %s }
          { ?item wdt:P194 ?org } UNION { ?item wdt:P208 ?org } UNION { ?org wdt:P1001 ?item }
          ?org wdt:P856 ?website .
        }''' % values):
        details[row['item']]['org_websites'].add(row['website'])
    return details


def fetch_infobox_websites(titles):
    websites = {}
    titles = sorted(set(titles))
    for i in range(0, len(titles), 50):
        data = get(HEWIKI_API, params=dict(
            action='query', prop='revisions', rvprop='content', rvslots='main',
            titles='|'.join(titles[i:i+50]), redirects=1, format='json', formatversion=2
        ))
        aliases = {}
        for r in data['query'].get('normalized', []) + data['query'].get('redirects', []):
            aliases[r['to']] = aliases.get(r['from'], r['from'])
        for page in data['query']['pages']:
            if 'revisions' not in page:
                continue
            text = page['revisions'][0]['slots']['main']['content']
            for m in INFOBOX_WEBSITE.finditer(text):
                url = URL.search(m.group(1))
                if url:
                    websites[aliases.get(page['title'], page['title'])] = url.group(0)
                    break
    return websites


def normalize(url):
    if not url:
        return None
    parsed = urlparse(url.strip())
    if not parsed.hostname or not parsed.hostname.isascii():
        return None
    return '%s://%s/' % (parsed.scheme or 'http', parsed.hostname.lower())


def host(url):
    return urlparse(url).hostname.removeprefix('www.')


def resolve(hostname):
    try:
        status = get(DNS_API, params=dict(name=hostname, type='A'))['Status']
    except Exception:
        logging.exception('DNS lookup failed for %s', hostname)
        return 'dns-error'
    # 0 = NOERROR, 3 = NXDOMAIN; anything else is a server-side problem, not a dead domain
    return {0: 'ok', 3: 'dns-failure'}.get(status, 'dns-error')


def resolve_all(hosts, timeout=60):
    # Some lookups take tens of seconds, so run them side by side and give up on stragglers
    executor = ThreadPoolExecutor(8)
    futures = {host: executor.submit(resolve, host) for host in set(hosts)}
    wait(futures.values(), timeout=timeout)
    executor.shutdown(wait=False, cancel_futures=True)
    return {host: f.result() if f.done() else 'dns-timeout' for host, f in futures.items()}


def is_official(url):
    return urlparse(url).hostname.endswith(OFFICIAL_SUFFIXES)


SOURCES = ['wikidata-org', 'hewiki-infobox', 'wikidata']


def rank(url, source, dns):
    # Prefer live domains, then municipal domains, then the more reliable sources
    hostname = urlparse(url).hostname
    return (dns.get(hostname) != 'ok', not hostname.endswith('.muni.il'), not is_official(url),
            SOURCES.index(source), url)


def municipal_websites():
    crosswalk = read_csv('wikidata-crosswalk.csv')
    overrides = read_csv('overrides.csv')
    authorities = fetch_authorities()
    codes = [str(a['LocalAuthorityCode']) for a in authorities]
    qids = fetch_wikidata_items(codes, crosswalk)
    details = fetch_wikidata_details(set(qids.values()))
    infobox = fetch_infobox_websites(d['hewiki'] for d in details.values() if d['hewiki'])

    rows = []
    for authority in authorities:
        code = str(authority['LocalAuthorityCode'])
        qid = qids.get(code)
        d = details.get(qid, dict(item_websites=set(), org_websites=set(), hewiki=None))
        candidates = [
            (source, url)
            for source, urls in (
                ('wikidata-org', d['org_websites']),
                ('hewiki-infobox', [infobox.get(d['hewiki'])]),
                ('wikidata', d['item_websites']),
            )
            for url in set(filter(None, map(normalize, urls)))
        ]
        rows.append((authority, code, qid, d, normalize(overrides.get(code, {}).get('website')), candidates))

    dns = resolve_all(
        urlparse(url).hostname
        for *_, override, candidates in rows
        for url in [override] + [url for _, url in candidates] if url
    )

    for authority, code, qid, d, override, candidates in rows:
        candidates.sort(key=lambda c: rank(c[1], c[0], dns))
        best = {}
        for source, url in candidates:
            best.setdefault(source, url)
        if override:
            source, website = 'override', override
        else:
            source, website = candidates[0] if candidates else (None, None)

        reasons = []
        if not qid:
            reasons.append('no-wikidata-item')
        if not website:
            reasons.append('missing')
        else:
            if dns.get(urlparse(website).hostname) != 'ok':
                reasons.append(dns.get(urlparse(website).hostname, 'dns-error'))
            if not is_official(website):
                reasons.append('unofficial-domain')
        if source != 'override' and len(set(host(url) for _, url in candidates)) > 1:
            reasons.append('sources-disagree')

        yield dict(
            authority_code=code,
            entity_id=str(authority['LocalAuthorityHPNumber']),
            name=authority['LocalAuthorityName'],
            long_name=authority['LocalAuthorityLongName'],
            authority_type=authority['LocalAuthorityType'],
            district=authority['LocalAuthorityDistrict'],
            website=website,
            website_source=source,
            wikidata_id=qid,
            hewiki_title=d['hewiki'],
            wikidata_org_website=best.get('wikidata-org'),
            hewiki_infobox_website=best.get('hewiki-infobox'),
            wikidata_website=best.get('wikidata'),
            override_website=override,
            needs_review=bool(reasons),
            review_reasons=','.join(reasons) or None,
        )


def flow(*_):
    return DF.Flow(
        municipal_websites(),
        DF.update_resource(-1, name='municipal_websites', path='municipal_websites.csv'),
        DF.set_type('needs_review', type='boolean'),
        DF.set_primary_key(['authority_code']),
        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.dump_to_path('/var/datapackages/entities/municipal_websites'),
        DF.dump_to_sql(dict(
            municipal_websites={
                'resource-name': 'municipal_websites',
            }
        )),
    )


if __name__ == '__main__':
    DF.Flow(
        municipal_websites(),
        DF.update_resource(-1, name='municipal_websites', path='municipal_websites.csv'),
        DF.set_type('needs_review', type='boolean'),
        DF.dump_to_path(os.environ.get('OUT', 'municipal_websites_out')),
        DF.printer(num_rows=5),
    ).process()
