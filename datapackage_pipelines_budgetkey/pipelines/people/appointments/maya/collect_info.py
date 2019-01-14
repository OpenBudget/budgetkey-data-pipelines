import logging
import requests
from pyquery import PyQuery as pq
from datetime import datetime

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.object_storage import object_storage
from datapackage_pipelines_budgetkey.pipelines.people.appointments.maya.maya_nomination_form import MayaForm, ParseError

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

session = requests.Session()


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {'name': 'HeaderEntityNameEB', 'type': 'number'},
        {'name': 'HeaderProofValue', 'type': 'number'},
        {'name': 'HeaderProof', 'type': 'number'},
        {'name': 'notification_type', 'type': 'string'},
        {'name': 'HeaderFixtReport', 'type': 'number'},
        {'name': 'HeaderProofFormat', 'type': 'number'},
        {'name': 'HeaderProofValue_equals_HeaderProof', 'type':'number'},

        {'name': 'TaarichTchilatHaCehuna', 'type': 'number'},
        {'name': 'TaarichTchilatCehuna', 'type': 'number'},
        {'name': 'TaarichTehilatCehuna', 'type': 'number'},
        {'name': 'TaarichTchilatHaKehuna', 'type': 'number'},
        {'name': 'TaarichTchilatKehuna', 'type': 'number'},
        {'name': 'TaarichTehilatKehuna', 'type': 'number'},

        {'name': 'Gender', 'type': 'number'},
        {'name': 'Min', 'type': 'number'},
        {'name': 'gender', 'type': 'number'},

        {'name': 'Shem', 'type': 'number'},
        {'name': 'ShemPratiVeMishpacha', 'type': 'number'},
        {'name': 'ShemPriatiVeMishpacha', 'type': 'number'},
        {'name': 'ShemMishpahaVePrati', 'type': 'number'},
        {'name': 'ShemRoeCheshbon', 'type': 'number'},
        {'name': 'ShemRoehHeshbon', 'type': 'number'},
        {'name': 'Accountant', 'type': 'number'}

    ])
    return dp



def process_row(row, *_):
    s3_object_name = row['s3_object_name']
    url = object_storage.urlfor(s3_object_name)
    try:
        if object_storage.exists(s3_object_name):
            conn = session.get(url)
            pg = pq(conn.text, parser='html')

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
                'Min': len(pg.find("[fieldalias=Min]")),
                'gender': len(pg.find("[fieldalias=gender]")),

                'Shem': len(pg.find("[fieldalias=Shem]")),
                'ShemPratiVeMishpacha': len(pg.find("[fieldalias=ShemPratiVeMishpacha]")),
                'ShemPriatiVeMishpacha': len(pg.find("[fieldalias=ShemPriatiVeMishpacha]")),
                'ShemMishpahaVePrati': len(pg.find("[fieldalias=ShemMishpahaVePrati]")),
                'ShemRoeCheshbon': len(pg.find("[fieldalias=ShemRoeCheshbon]")),
                'ShemRoehHeshbon': len(pg.find("[fieldalias=ShemRoehHeshbon]")),
                'Accountant': len(pg.find("[fieldalias=Accountant]")),
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
