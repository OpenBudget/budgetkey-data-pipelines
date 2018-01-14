import re
import itertools

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew

cat_re = re.compile('C[0-9]+')


def close(cats, rows, source, property):
    rows = ''.join(rows)
    for cat in cats:
        yield {
            'budget_code': cat,
            property: rows,
            'source': source
        }


def process_file(filename, property):
    cats = None
    rows = []
    source = None

    for line in open(filename):

        line = line.strip()
        if not line:
            continue

        if line.startswith('### '):
            if cats:
                yield from close(cats, rows, source, property)
            cats = cat_re.findall(line)
            rows = []
            source = 'צוות מפתח התקציב'
        elif line.startswith('#### '):
            source = line[5:]
        elif line.startswith('- ') or line.startswith('* '):
            rows.append('<li>{}</li>'.format(line[2:]))
        else:
            rows.append('<p>{}</p>'.format(line))

    yield from close(cats, rows, source, property)


def main():
    params, dp, res_iter = ingest()

    dp['name'] = 'category-explanations'
    dp['resources'] = [{
        'name': 'category-explanations',
        'path': 'data/category-explanations.csv',
        PROP_STREAMING: True,
        'schema': {
            'fields': [
                {'name': 'budget_code', 'type': 'string'},
                {'name': 'explanation', 'type': 'string'},
                {'name': 'explanation_short', 'type': 'string'},
                {'name': 'source', 'type': 'string'},
            ]
        }
    }]

    spew(dp, [
        itertools.chain(
            process_file('category-explanations.md', 'explanation'),
            process_file('category-explanations-short.md', 'explanation_short'),
        )
    ])

if __name__=="__main__":
    main()