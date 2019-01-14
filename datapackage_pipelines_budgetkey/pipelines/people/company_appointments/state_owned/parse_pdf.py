"""
Parses a single Input resource and adds a column with the entrie content of the PDF in a single large array

Expected Parameters

target: the name of the column to add

source: The name of the resource and field to work on
  name = the exact name of the resource
  field = the field that contains a url of the PDF to parse

tabula_params: (optional)
    the parameters to pass to tabula

transpose: (Optional) if true the PDF id transposed. I.E. the rows are converted to columns

"""

import tabula
import tempfile
import requests
import shutil
import os
import logging
import re

from datapackage_pipelines.wrapper import ingest, spew


logging.getLogger("requests").setLevel(logging.WARNING)


parameters, dp, res_iter = ingest()
target = parameters['target']
source = parameters['source']

HEADER_SEARCH_ROWS = 10
SCORE_THRESHOLD = 80

class ParseError(Exception):
    pass



def tabula_pdf_parser(url, transpose = False, tabula_params = None):
    file_name = url
    temp_file = None
    data = []
    params =  {
        'guess': False,
        'output_format': 'json'
    }


    try:
        if (url.startswith('http')):
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            stream = requests.get(url, stream=True, verify=False).raw
            shutil.copyfileobj(stream, temp_file)
            temp_file.close()
            file_name = temp_file.name

        if tabula_params:
            params.update(tabula_params)

        tables = tabula.read_pdf(file_name, **params)
        for table in tables:
            selection_data = [[re.sub(r'\s', ' ', re.sub(r'-\r|\n', ' ',cell.get('text',''))).strip()
                                      for cell in row]
                                        for row in table['data']]
            if transpose:
                selection_data = list(map(list, zip(*selection_data)))
            data.extend(selection_data)


        return data
    finally:
        if temp_file:
            os.unlink(temp_file.name)


def process_pdf_in_resource(resource):
    for row in resource:
        all_pdf_records = tabula_pdf_parser(row[source['key']], parameters.get('transpose', False), parameters.get('tabula_params') )
        for table_record in all_pdf_records:
            new_record = {k:v for k,v in row.items() }
            new_record[target] = table_record
            yield new_record


def new_resource_iterator(res_iter):
    for resource in res_iter:
        name = resource.spec['name']
        if name == source['name']:
            yield process_pdf_in_resource(resource)
        else:
            yield resource

def modify_datapackage(datapackage):
    found_resource = False
    for resource in datapackage['resources']:
        name = resource['name']
        if name == source['name']:
            found_resource = True
            resource['schema']['fields'].extend([
                {
                    'name': target,
                    'type': 'array'
                }
            ])
    assert found_resource is True, "Failed to find resource to work on"
    return datapackage


spew(modify_datapackage(dp), new_resource_iterator(res_iter))


