"""dpp flow: links one source's documents to budgetkey records (see linker.py and README.md).

Parameters: source ('state_comptroller' or 'gov_decisions'), out-path.
Local runs: python -m datapackage_pipelines_budgetkey.pipelines.related_items.link_source <source> with
OUT (output dir), LIMIT, DOC_IDS (comma separated source doc ids), and for the comptroller CHAPTERS (the chapters
datapackage dir); decisions are read from the DB (or the API, locally) with DECISIONS_WHERE as an extra SQL filter.
"""
import csv
import logging
import os
import sys

import dataflows as DF

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_db
from datapackage_pipelines_budgetkey.pipelines.related_items import sources as S
from datapackage_pipelines_budgetkey.pipelines.related_items.linker import (
    LINK_FIELDS, MENTION_FIELDS, Targets, link_documents)

CHAPTERS_PATH = '/var/datapackages/state_comptroller/chapters'
DECISIONS_PATH = '/var/datapackages/government_decisions'
DECISION_COLUMNS = ['id', 'title', 'text', 'doc_published_date', 'publish_date', 'office', 'government',
                    'policy_type', 'procedure_number_str']


def load(path, resource):
    csv.field_size_limit(1 << 30)  # chapters' full texts
    return DF.Flow(DF.load(os.path.join(path, 'datapackage.json'), resources=[resource])).results()[0][0]


def documents(source):
    if source == 'state_comptroller':
        path = os.environ.get('CHAPTERS') or CHAPTERS_PATH
        return list(S.comptroller_documents(load(path, 'audits'), load(path, 'findings')))
    if source == 'gov_decisions':
        where = os.environ.get('DECISIONS_WHERE')
        if os.path.exists(os.path.join(DECISIONS_PATH, 'datapackage.json')) and not where:
            rows = load(DECISIONS_PATH, 'gov_decisions')
        else:
            rows = ri_db.query('select {} from government_decisions{}'.format(
                ', '.join(DECISION_COLUMNS), ' where ' + where if where else ''), DECISION_COLUMNS)
        return list(S.decision_documents(rows))
    raise ValueError(source)


def select(docs):
    ids = [x for x in (os.environ.get('DOC_IDS') or '').split(',') if x]
    if ids:
        docs = [d for d in docs if d['doc_id'] in ids]
    limit = int(os.environ.get('LIMIT') or 0)
    return docs[:limit] if limit else docs


def run(source):
    docs = select(documents(source))
    logging.info('%s: %d documents', source, len(docs))
    targets = Targets.load()
    links, mentions, usage = link_documents(docs, targets)
    logging.info('%s: %d links, %d mentions; LLM usage %s', source, len(links), len(mentions), usage.by_model)
    return links, mentions


def output_flow(links, mentions, out_path):
    def schema(fields):
        return [DF.set_type(name, type=t, resources=-1) for name, t in fields]

    return DF.Flow(
        links, DF.update_resource(-1, name='related_items', path='related_items.csv'),
        DF.select_fields([f for f, _ in LINK_FIELDS], resources='related_items'),
        *schema(LINK_FIELDS),
        DF.set_primary_key(['link_id'], resources='related_items'),
        mentions, DF.update_resource(-1, name='related_items_mentions', path='related_items_mentions.csv'),
        DF.select_fields([f for f, _ in MENTION_FIELDS], resources='related_items_mentions'),
        *schema(MENTION_FIELDS),
        DF.dump_to_path(out_path),
    )


def flow(parameters, *_):
    links, mentions = run(parameters['source'])
    return DF.Flow(output_flow(links, mentions, parameters['out-path']),
                   DF.update_resource(None, **{'dpp:streaming': True}))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    for name in ('httpx', 'google_genai', 'openai', 'urllib3'):
        logging.getLogger(name).setLevel(logging.WARNING)
    source = sys.argv[1]
    links, mentions = run(source)
    output_flow(links, mentions, os.environ.get('OUT') or 'related_items_' + source).process()
