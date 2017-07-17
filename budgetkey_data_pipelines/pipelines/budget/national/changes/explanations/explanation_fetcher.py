import itertools
import logging
import os
import tarfile
import zipfile
import tempfile
import shutil
import requests
import base64
from textract.parsers.doc_parser import Parser
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

logging.getLogger().setLevel(logging.INFO)


class DocParser(Parser):
    def decode(self, text):
        return text.decode('utf-8', errors='ignore')

    def encode(self, text, encoding):
        return text


def get_explanations(url):
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as archive:
        logging.info('Connecting to %r', url)
        resp = requests.get(url, stream=True)
        stream = resp.raw
        shutil.copyfileobj(stream, archive)
        archive = open(archive.name, 'rb')

        if '.tar.gz' in url:
            t_archive = tarfile.open(fileobj=archive, mode='r|gz')
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

        os.unlink(archive.name)


resource = parameters['resource']
schema = {
    'fields': [
        {'name': 'contents', 'type': 'string'},
        {'name': 'orig_name', 'type': 'string'},
    ]
}
resource['schema'] = schema
dp['resources'].append(resource)

spew(dp, itertools.chain(res_iter, [get_explanations(parameters['url'])]))
