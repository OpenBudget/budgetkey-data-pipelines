import logging
import requests

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.object_storage import object_storage
from datapackage_pipelines_budgetkey.pipelines.people.appointments.maya.maya_nomination_form import MayaForm


def modify_datapackage(datapackage, parameters, stats):
    datapackage['resources'][0]['schema']['fields'].extend([
        {'name': 'name','type': 'string'},
        {'name': 'start_date', 'type': 'date'},
        {'name': 'organisation_name','type': 'string'},
        {'name': 'gender', 'type': 'string'},
        {'name': 'source', 'type': 'string'},
        {'name': 'positions', 'type': 'array'}
    ])
    return datapackage


def process_row(row, *_):
    s3_object_name = row['s3_object_name']
    maya_form = MayaForm(object_storage.urlfor(s3_object_name))
    try:
        row.update({
            'source': 'maya.tase.co.il',
            'organisation_name': maya_form.company,
            'start_date' : maya_form.position_start_date,
            'positions': maya_form.positions,
            'gender': maya_form.gender,
            'name': maya_form.full_name,
        })
    except ValueError as err:
        raise ValueError("Failed to parse object {}".format(url)) from err
    return row

process(process_row=process_row, modify_datapackage=modify_datapackage)
