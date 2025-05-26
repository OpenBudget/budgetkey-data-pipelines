import itertools
import logging
import os
import tarfile
import zipfile
import tempfile
import shutil
import requests
import base64

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from textract.parsers.doc_parser import Parser
from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver

parameters, dp, res_iter = ingest()

# logging.getLogger().setLevel(logging.INFO)

headers = {
    'User-Agent': 'kz-data-reader'
}
class DocParser(Parser):
    def decode(self, text):
        return text.decode('utf-8', errors='ignore')

    def encode(self, text, encoding):
        return text


def get_explanations(url):
    logging.info('Connecting to %r', url)
    try:
        resp = requests.get(url, stream=True, timeout=300, headers=headers).raw
        outfile = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.basename(url))
        shutil.copyfileobj(resp, outfile)
        archive = outfile.name
    except Exception:
        gcl = google_chrome_driver()
        archive = gcl.download(url)
        gcl.teardown()

    if '.tar.gz' in url:
        t_archive = tarfile.open(name=archive, mode='r|gz')
        files = ((os.path.basename(member.name), t_archive.extractfile(member))
                    for member in t_archive
                    if member is not None and member.isfile())
    elif '.zip' in url:
        z_archive = zipfile.ZipFile(archive)
        files = ((os.path.basename(member.filename), z_archive.open(member))
                    for member in z_archive.filelist)
    else:
        assert False

    for name, item in files:
        contents = base64.b64encode(item.read()).decode('ascii')
        yield {'contents': contents, 'orig_name': name}

    os.unlink(archive)

resource = parameters['resource']
resource[PROP_STREAMING] = True
schema = {
    'fields': [
        {'name': 'contents', 'type': 'string'},
        {'name': 'orig_name', 'type': 'string'},
    ]
}
resource['schema'] = schema
dp['resources'].append(resource)

spew(dp, itertools.chain(res_iter, [get_explanations(parameters['url'])]))
