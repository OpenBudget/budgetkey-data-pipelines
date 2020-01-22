import base64
import itertools
import logging
import tempfile

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from textract.parsers.doc_parser import Parser
from textract.exceptions import ShellError
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

logging.getLogger().setLevel(logging.INFO)


class DocParser(Parser):
    def decode(self, text):
        return text.decode('utf-8', errors='ignore')

    def encode(self, text, encoding):
        return text


def get_explanations(res_iter_):

    for res in res_iter_:
        for row in res:
            orig_name = row['orig_name']
            with tempfile.NamedTemporaryFile(mode='wb', suffix=orig_name) as tmp:
                tmp.write(base64.b64decode(row['contents'].encode('ascii')))
                filename = tmp.name
                try:
                    text = DocParser().process(filename, '')
                except ShellError:
                    logging.exception('Error with filename %s', orig_name)
                    text = ''

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

                parts = orig_name.split(".")[0].split("_")
                parts = map(int, parts)
                year, leading_item, req_code = parts
                yield {
                    'year': year,
                    'leading_item': leading_item,
                    'req_code': req_code,
                    'explanation': text.strip()
                }
                logging.info('Parsed filename %s', orig_name)



resource = parameters['resource']
resource[PROP_STREAMING] = True
schema = {
    'fields': [
        {'name': 'year', 'type': 'integer'},
        {'name': 'leading_item', 'type': 'integer'},
        {'name': 'req_code', 'type': 'integer'},
        {'name': 'explanation', 'type': 'string', 'es:hebrew': True},
    ]
}
resource['schema'] = schema
dp['resources'] = [resource]

spew(dp, [get_explanations(res_iter)])
