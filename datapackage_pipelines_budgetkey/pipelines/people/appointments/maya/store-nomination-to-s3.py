import logging
import time
import requests
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.object_storage import object_storage
from datapackage_pipelines_budgetkey.pipelines.people.appointments.maya.maya_nomination_form import MayaForm

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

session = requests.Session()



###
#  The tase web site encodes the files in the single byte hebrew code page cp1255
#  however it stores this information inside the html file. in a meta tag, the web server
#  will return ISO-8859-1 as the code page (the Latin1 single byte code page)
#
#  this will cause tools such as pyQuery problems with parsing the data. In the best case
#  scenario all the hebrew text gets mangled in the worst case some of the tags can't be accessed.
#
#  to do that we need to decode the content from the existing code page
###
def get_charset(bytes, default="windows-1255"):

    # fortunately pq can parse the header ok.. even if the code page is all wrong
    page = pq(bytes)
    elem = page.find('meta[http-equiv="Content-Type"]')
    if len(elem) == 0:
        return default

    content = pq(elem[0]).attr('content')

    for str in content.split(';'):
        str = str.strip()
        if (str.startswith('charset')):
            return str.split('=')[-1]
    return default



def process_row(row, *_):
    s3_object_name = row['s3_object_name']
    url = row['url']
    if not object_storage.exists(s3_object_name):
        conn = session.get(url)
        time.sleep(3)
        if not conn.status_code == requests.codes.ok:
            return None

        charset = get_charset(conn.content)
        conn.encode = charset
        object_storage.write(s3_object_name,
                             data=conn.content,
                             public_bucket=True,
                             create_bucket=True,
                             content_type="text/html; charset={}".format(charset))
    return row



process(process_row=process_row)


