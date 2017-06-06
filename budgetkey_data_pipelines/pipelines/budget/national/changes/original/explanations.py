import itertools
import logging
import os
import tarfile
import zipfile
import tempfile
import shutil
import requests
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
    resp = requests.get(url, stream=True)
    stream = resp.raw
    if '.tar.gz' in url:
        t_archive = tarfile.open(fileobj=stream, mode='r|gz')
        files = ((os.path.basename(member.name), t_archive.extractfile(member))
                 for member in t_archive
                 if member is not None and member.isfile())
    elif '.zip' in url:
        tmp = tempfile.NamedTemporaryFile()
        shutil.copyfileobj(stream, tmp)
        z_archive = zipfile.ZipFile(tmp)
        files = ((os.path.basename(member.filename), z_archive.open(member))
                 for member in z_archive.filelist)
    else:
        assert False

    for name, item in files:
        logging.info('Got filename %s', name)
        with tempfile.NamedTemporaryFile(suffix=name, mode='wb', delete=False) as tmp:
            shutil.copyfileobj(item, tmp)
            tmp.close()
            text = DocParser().process(tmp.name, '')

            lines = text.split('\n')
            lines = itertools.takewhile(
                lambda line: all(x not in line
                                 for x in ['בברכה', 'בכבוד רב']),
                itertools.dropwhile(
                    lambda line: 'הנדון' not in line,
                    lines
                )
            )
            text = '\n'.join(lines)

            parts = name.split(".")[0].split("_")
            parts = map(int, parts)
            year, leading_item, req_code = parts
            yield {
                'year': year,
                'leading_item': leading_item,
                'req_code': req_code,
                'explanation': text.strip()
            }
            os.unlink(tmp.name)


resource = parameters['resource']
schema = {
    'fields': [
        {'name': 'year', 'type': 'integer'},
        {'name': 'leading_item', 'type': 'integer'},
        {'name': 'req_code', 'type': 'integer'},
        {'name': 'explanation', 'type': 'string'},
    ]
}
resource['schema'] = schema
dp['resources'].append(resource)

spew(dp, itertools.chain(res_iter, [get_explanations(parameters['url'])]))
