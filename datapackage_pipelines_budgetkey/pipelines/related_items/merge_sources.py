"""dpp flow: unions the per-source outputs into the related_items and related_items_mentions tables.

Parameters: sources (datapackage dirs written by link_source), out-path, and optionally issues: the State
Comptroller issues datapackage. With it, links made from findings are lifted to the issues the findings belong to
(`state_comptroller_issues/{issue_id}`, method `derived:issue`), so an issue's page can list what it is about across
all its reports.
"""
import csv
import hashlib
import os

import dataflows as DF

from datapackage_pipelines_budgetkey.pipelines.related_items.linker import (
    LINK_FIELDS, MENTION_FIELDS, RELATION_ORDER, VERSION)


def load_issues(path):
    """({finding_id: issue_id}, {issue_id: title}) from the issues datapackage, or empty maps."""
    members, titles = {}, {}
    if not path or not os.path.exists(os.path.join(path, 'issue_members.csv')):
        return members, titles
    csv.field_size_limit(1 << 30)
    with open(os.path.join(path, 'issue_members.csv'), encoding='utf8') as f:
        for r in csv.DictReader(f):
            if r['member_type'] == 'finding':
                members[r['member_id']] = r['issue_id']
    with open(os.path.join(path, 'issues.csv'), encoding='utf8') as f:
        for r in csv.DictReader(f):
            titles[r['issue_id']] = r['title']
    return members, titles


def issue_links(rows, members, titles):
    """Rows, then one derived row per (issue, target) for the targets linked from the issue's findings. A derived
    row keeps the strongest of its findings' links, and the finding it came from as source_part_id."""
    lifted = {}
    for row in rows:
        yield row
        issue = members.get(row.get('source_part_id') or '')
        if row.get('source_kind') != 'state_comptroller_audits' or not issue:
            continue
        key = (issue, row['target_doc_id'])
        old = lifted.get(key)
        if old is None or row['confidence'] > old['confidence'] or (
                row['confidence'] == old['confidence']
                and RELATION_ORDER.index(row['relation']) < RELATION_ORDER.index(old['relation'])):
            lifted[key] = row
    for (issue, target), row in lifted.items():
        source = 'state_comptroller_issues/' + issue
        yield dict(row, source_doc_id=source, source_kind='state_comptroller_issues',
                   source_title=titles.get(issue) or row.get('source_title'), method='derived:issue',
                   link_id=hashlib.sha1('|'.join([source, row['source_part_id'], target]).encode('utf8'))
                   .hexdigest()[:16], linker_version=VERSION)


def flow(parameters, *_):
    members, titles = load_issues(parameters.get('issues'))
    steps = []
    for name, fields in (('related_items', LINK_FIELDS), ('related_items_mentions', MENTION_FIELDS)):
        # concatenate wants the resources it merges next to each other.
        for path in parameters['sources']:
            steps.append(DF.load(os.path.join(path, 'datapackage.json'), resources=[name]))
        steps.append(DF.concatenate({f: [] for f, _ in fields}, target=dict(name=name, path=name + '.csv'),
                                    resources=name))
        steps.extend(DF.set_type(f, type=t, resources=name) for f, t in fields)
    if members:
        steps.append(_on_resource('related_items', lambda rows: issue_links(rows, members, titles)))
    steps.append(DF.set_primary_key(['link_id'], resources='related_items'))
    steps.append(DF.update_resource(None, **{'dpp:streaming': True}))
    steps.append(DF.dump_to_path(parameters['out-path']))
    if parameters.get('dump-to-sql', True):
        from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic
        steps.append(dump_to_sql_atomic(dict(
            related_items={'resource-name': 'related_items'},
            related_items_mentions={'resource-name': 'related_items_mentions'},
        )))
    return DF.Flow(*steps)


def _on_resource(name, func):
    def step(package):
        yield package.pkg
        for resource in package:
            if resource.res.name == name:
                yield func(resource)
            else:
                yield resource
    return step
