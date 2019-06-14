import logging
import requests
import random
from pyquery import PyQuery as pq
import os

from datetime import datetime

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.object_storage import object_storage

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

session = requests.Session()


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {'name': 'HeaderEntityNameEB', 'type': 'number'},
        {'name': 'HeaderProofValue', 'type': 'number'},

        {'name': 'HeaderProof', 'type': 'number'},
        {'name': 'HeaderProofValue_equals_HeaderProof', 'type':'number'},
        {'name': 'HeaderFixtReport', 'type': 'number'},
        {'name': 'HeaderProofFormat', 'type': 'number'},


        {'name': 'notification_type', 'type': 'string'},



        {'name': 'positions', 'type': 'array'},

        {'name': 'alias_stats', 'type':'array'}
    ])
    return dp


def needs_decoding(txt):
    proof = pq(txt).find("#HeaderProof").text()
    return 'א' not in proof and 'R' not in proof and 'r' not in proof

def first_alias_as_string(pg, aliases):
    try:
        return next(filter(lambda x: len(x) > 0,(pg.find("[fieldalias={}]".format(it)).text().strip() for it in aliases)))
    except StopIteration:
        return ""

def all_aliases_as_string(pg, aliases):
    return ', '.join(filter(lambda x: len(x) > 0,(pg.find("[fieldalias={}]".format(it)).text().strip() for it in aliases)))

def _findByTextAlias(e, aliaes):
    selector = ",".join(["[fieldalias={}]".format(a) for a in aliaes])
    return e.find(selector)

def get_positions_array(pg):
    aliases = ['Tafkid', 'HaTafkidLoMuna', 'HaTafkid1']
    desc_aliases = ['TeurTafkid', 'TeurHaTafkidLoMuna', 'HaTafkid2']
    elems = _findByTextAlias(pg, aliases)

    def extract_title(elem):
        row = elem.closest('tr')
        origin_elem = _findByTextAlias(pq(row[0]), aliases)
        desc_elem = _findByTextAlias(pq(row[0]), desc_aliases)
        return [origin_elem.text().strip(), desc_elem.text().strip()]

    return [extract_title(pq(it)) for it in elems]

def collect_all_aliases(qry):
    elems = qry.find("[fieldalias]")
    return [ {pq(e).attr("fieldalias").strip(): pq(e).text().strip() } for e in elems ]


def process_row(row, *_):
#    if random.uniform(0, 1) >= 0.005:
#        return None

    s3_object_name = row['s3_object_name']
    #url = os.path.join("https://ams3.digitaloceanspaces.com", "budgetkey-files", s3_object_name)
    url = object_storage.urlfor(s3_object_name)
    try:
        if object_storage.exists(s3_object_name):
            conn = session.get(url)
            txt = conn.text
            if needs_decoding(txt):
                txt = conn.content.decode('utf-8')
            pg = pq(txt, parser='html')

            row.update({
                'url': url,
                'HeaderEntityNameEB': len(pg.find('#HeaderEntityNameEB')),
                'HeaderProofValue': len(pg.find('#HeaderProofValue')),
                'HeaderProof': len(pg.find('#HeaderProof ~ span:first')),
                'HeaderProofValue_equals_HeaderProof': pg.find('#HeaderProof ~ span:first').text().strip() == pg.find('#HeaderProofValue').text().strip(),

                'HeaderFixtReport': len(pg.find('#HeaderFixtReport')),
                'HeaderProofFormat': len(pg.find("#HeaderProofFormat")),

                'notification_type': pg.find('#HeaderFormNumber').text().strip(),



                'positions': get_positions_array(pg),

                'alias_stats': collect_all_aliases(pg)

            })
        else:
            return None
    except Exception as err:
        raise RuntimeError('Parsing Failed Unexpectedly on {}'.format(url)) from err
    return row


process(process_row=process_row, modify_datapackage=modify_datapackage)

