import logging
import requests

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.object_storage import object_storage
from datapackage_pipelines_budgetkey.pipelines.people.appointments.maya.maya_nomination_form import MayaForm

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

session = requests.Session()

def modify_datapackage(datapackage, parameters, stats):
    datapackage['resources'][0]['schema']['fields'].extend([
        {'name': 'source', 'type': 'string'},

        #common fields
        {'name': 'organisation_name','type': 'string'},
        {'name': 'id', 'type':'string'},
        {'name': 'notification_type', 'type':'string'},
        {'name': 'fix_for', 'type':'string'},
        {'name': 'is_nomination', 'type': 'boolean' },

        #Specific to nominations
        {'name': 'start_date', 'type': 'date'},
        {'name': 'positions', 'type': 'array'},
        {'name': 'gender', 'type': 'string'},
        {'name': 'name','type': 'string'},


    ])
    return datapackage


def process_row(row, *_):
    s3_object_name = row['s3_object_name']
    url = object_storage.urlfor(s3_object_name)
    conn = session.get(url)
    maya_form = MayaForm(conn.text)
    try:
        row.update({
            'source': 'maya.tase.co.il',

            'organisation_name': maya_form.company,
            'id': maya_form.id,
            'notification_type': maya_form.type,
            'fix_for': maya_form.fix_for,
            'is_nomination': False,
            'start_date': None,
            'positions':"",
            'gender':"",
            'name':""
        })

        if maya_form.is_nomination:
            row.update({
                'is_nomination': True,
                'start_date' : maya_form.position_start_date,
                'positions': maya_form.positions,
                'gender': maya_form.gender,
                'name': maya_form.full_name
            })
    except ValueError as err:
        raise ValueError("Failed to parse object {}".format(url)) from err
    return row

process(process_row=process_row, modify_datapackage=modify_datapackage)
