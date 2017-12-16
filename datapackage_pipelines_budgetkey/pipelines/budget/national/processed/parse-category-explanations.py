import re

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.wrapper import spew

cat_re = re.compile('C[0-9]+')


def close(cats, rows, source):
    rows = ''.join(rows)
    for cat in cats:
        yield dict(
            budget_code=cat,
            explanation=rows,
            source=source
        )


def process_file():
    cats = None
    rows = []
    source = None

    for line in open('category-explanations.md'):

        line = line.strip()
        if not line:
            continue

        if line.startswith('### '):
            if cats:
                yield from close(cats, rows, source)
            cats = cat_re.findall(line)
            rows = []
            source = 'צוות מפתח התקציב'
        elif line.startswith('#### '):
            source = line[5:]
        elif line.startswith('- '):
            rows.append('<li>{}</li>'.format(line))
        else:
            rows.append('{}<br/>'.format(line))

    yield from close(cats, rows, source)


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
                {'name': 'source', 'type': 'string'},
            ]
        }
    }]

    spew(dp, [process_file()])

if __name__=="__main__":
    main()