# coding=utf-8
from dataflows import Flow, printer, add_field, load
import csv
from collections import defaultdict
import logging

def link_to_next_report(rows):
    memory = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        type = row['type']
        company = row['document']['HeaderMisparBaRasham'][0]

        old_record = memory[type][company]
        if old_record:
            old_record['next_doc'] = row['id']
            row['prev_doc'] = old_record['id']
            yield old_record
        memory[type][company] = row

    for by_type in memory.values():
        for record in by_type.values():
            record['next_doc'] = None
            yield record

def flow(*_):
    return Flow(
        add_field('next_doc', 'string'),
        add_field('prev_doc', 'string'),
        link_to_next_report
    )

if __name__ == '__main__':
    csv.field_size_limit(512*1024)

    Flow(
        load('/var/datapackages/maya/maya_complete_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()

