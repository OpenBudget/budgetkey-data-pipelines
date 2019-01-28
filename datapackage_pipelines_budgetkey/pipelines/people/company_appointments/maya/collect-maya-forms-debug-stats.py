import logging
import requests
import json
from pyquery import PyQuery as pq

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

        {'name': 'notification_type', 'type': 'string'},
        {'name': 'HeaderFixtReport', 'type': 'number'},
        {'name': 'HeaderProofFormat', 'type': 'number'},

        {'name': 'TaarichTchilatHaCehuna', 'type': 'number'},
        {'name': 'TaarichTchilatCehuna', 'type': 'number'},
        {'name': 'TaarichTehilatCehuna', 'type': 'number'},
        {'name': 'TaarichTchilatHaKehuna', 'type': 'number'},
        {'name': 'TaarichTchilatKehuna', 'type': 'number'},
        {'name': 'TaarichTehilatKehuna', 'type': 'number'},

        {'name': 'Gender', 'type': 'number'},
        {'name': 'full_name', 'type': 'string'},
        {'name': 'gender', 'type': 'string'},
        {'name': 'positions', 'type': 'array'},

        {'name': 'Shem', 'type': 'number'},
        {'name': 'ShemPratiVeMishpacha', 'type': 'number'},
        {'name': 'ShemPriatiVeMishpacha', 'type': 'number'},
        {'name': 'ShemMishpahaVePrati', 'type': 'number'},
        {'name': 'ShemRoeCheshbon', 'type': 'number'},
        {'name': 'ShemRoehHeshbon', 'type': 'number'},
        {'name': 'Accountant', 'type': 'number'},
        {'name': 'Tapkid', 'type': 'number'},
        {'name': 'Tafkid', 'type': 'number'},
        {'name': 'HaTafkidLoMuna', 'type': 'number'},
        {'name': 'TeurTafkid', 'type': 'number'},
        {'name': 'LeloTeur', 'type': 'number'},
        {'name': 'TeurHaTafkidLoMuna', 'type': 'number'},
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
    aliases = ['Tapkid', 'Tafkid', 'HaTafkidLoMuna']
    desc_aliases = ['TeurTafkid', 'LeloTeur', 'TeurHaTafkidLoMuna']
    elems = _findByTextAlias(pg, aliases)

    def extract_title(elem):
        row = elem.closest('tr')
        origin_elem = _findByTextAlias(pq(row[0]), aliases)
        desc_elem = _findByTextAlias(pq(row[0]), desc_aliases)
        return [origin_elem.text().strip(), desc_elem.text().strip()]

    return [extract_title(pq(it)) for it in elems]

def process_row(row, *_):
    s3_object_name = row['s3_object_name']
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
                'notification_type': pg.find('#HeaderFormNumber').text().strip(),

                'HeaderFixtReport': len(pg.find('#HeaderFixtReport')),
                'HeaderProofFormat': len(pg.find("#HeaderProofFormat")),

                'TaarichTchilatHaCehuna': len(pg.find("[fieldalias=TaarichTchilatHaCehuna]")),
                'TaarichTchilatCehuna': len(pg.find("[fieldalias=TaarichTchilatCehuna]")),
                'TaarichTehilatCehuna': len(pg.find("[fieldalias=TaarichTehilatCehuna]")),
                'TaarichTchilatHaKehuna': len(pg.find("[fieldalias=TaarichTchilatHaKehuna]")),
                'TaarichTchilatKehuna': len(pg.find("[fieldalias=TaarichTchilatKehuna]")),
                'TaarichTehilatKehuna': len(pg.find("[fieldalias=TaarichTehilatKehuna]")),

                'Gender': len(pg.find("[fieldalias=Gender]")),
                'gender': pg.find("[fieldalias=Gender]").text().strip(),

                'Shem': len(pg.find("[fieldalias=Shem]")),
                'ShemPratiVeMishpacha': len(pg.find("[fieldalias=ShemPratiVeMishpacha]")),
                'ShemPriatiVeMishpacha': len(pg.find("[fieldalias=ShemPriatiVeMishpacha]")),
                'ShemMishpahaVePrati': len(pg.find("[fieldalias=ShemMishpahaVePrati]")),
                'ShemRoeCheshbon': len(pg.find("[fieldalias=ShemRoeCheshbon]")),
                'ShemRoehHeshbon': len(pg.find("[fieldalias=ShemRoehHeshbon]")),
                'Accountant': len(pg.find("[fieldalias=Accountant]")),
                'Tapkid':  len(pg.find("[fieldalias=Tapkid]")),
                'Tafkid':  len(pg.find("[fieldalias=Tafkid]")),
                'HaTafkidLoMuna':  len(pg.find("[fieldalias=HaTafkidLoMuna]")),
                'TeurTafkid':  len(pg.find("[fieldalias=TeurTafkid]")),
                'LeloTeur':  len(pg.find("[fieldalias=LeloTeur]")),
                'TeurHaTafkidLoMuna':  len(pg.find("[fieldalias=TeurHaTafkidLoMuna]")),

                'full_name': all_aliases_as_string(pg, ['Shem', 'ShemPratiVeMishpacha', 'ShemPriatiVeMishpacha',
                                                        'ShemMishpahaVePrati']),

                'positions': get_positions_array(pg),


                #'is_nomination': False,
                #'positions':"",
                #'gender':"",
                #'name':""
            })
        else:
            return None
    except Exception as err:
        raise RuntimeError('Parsing Failed Unexpectedly on {}'.format(url)) from err
    return row


process(process_row=process_row, modify_datapackage=modify_datapackage)
