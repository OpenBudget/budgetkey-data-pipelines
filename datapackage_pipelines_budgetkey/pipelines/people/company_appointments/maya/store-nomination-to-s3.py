import logging
import time
import requests
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.object_storage import object_storage
from datapackage_pipelines_budgetkey.pipelines.people.company_appointments.maya.maya_nomination_form import MayaForm

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

session = requests.Session()



###
#  The tase web site may encode files either in utf-8 or in windows-1255
#  however it never returns ContentEncoding header in the response. And although the page includes
#  a meta tag in the header with a content Type the value there is always windows-1255 regardless
#  of the actualy encoding in the file

#  The ugly solution is simply to try
###
def get_charset(conn, default="windows-1255"):
    try:
        #If utf-8 file then parsing will find the element and it should contain the an aleph.
        #in case of windows-1255 the content will be junk or impossible to find
        conn.encoding = "utf-8"
        if 'א' in pq(pq(conn.text).find("#HeaderProof")[0]).text():
            return "utf-8"
    except:
        pass
    return default


from datetime import datetime

counter = 0
st = datetime.now()

def process_row(row, *_):
    s3_object_name = row['s3_object_name']
    global counter
    global st

    counter += 1

    if not object_storage.exists(s3_object_name):
        url = row['url']
        conn = session.get(url)
        time.sleep(3)
        if not conn.status_code == requests.codes.ok:
            return None

        charset = get_charset(conn)
        conn.encode = charset
        object_storage.write(s3_object_name,
                             data=conn.content,
                             public_bucket=True,
                             create_bucket=True,
                             content_type="text/html; charset={}".format(charset))
    if counter % 100 == 0:
        logging.warning("Took {} for {} records".format( ( datetime.now() -st).total_seconds(), counter))
        st = datetime.now()
    return row



process(process_row=process_row)


