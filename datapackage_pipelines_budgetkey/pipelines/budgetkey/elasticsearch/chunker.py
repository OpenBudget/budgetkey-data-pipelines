import dataflows as DF
from dataflows.helpers.resource_matcher import ResourceMatcher
from pathlib import Path

from datapackage_pipelines_budgetkey.common.cached_openai import embed

def get_text(c, row):
    if isinstance(c, str):
        yield c.format(**row)
    elif isinstance(c, dict):
        text = row.get(c['field'])
        if text:
            if c.get('method') == 'chunk':
                chunk_size = c.get('chunk_size') or 128
                chunk_overlap = c.get('chunk_overlap') or 64
                while len(text) > 0:
                    yield text[:chunk_size]
                    text = text[chunk_size - chunk_overlap:]

def chunker(config, resource=None):
    matcher = ResourceMatcher(resource, None)
    def func(rows: DF.ResourceWrapper):
        if not matcher.match(rows.res.name):
            yield from rows
            return
        hits = 0
        total = 0
        for row in rows:
            vectors = []
            for c in config:
                for chunk in get_text(c, row):
                    hit, embedding = embed(chunk)
                    vectors.append(dict(embeddings=embedding))
                    if hit:
                        hits += 1
                    total += 1
                    if total % 1000 == 0:
                        hit_pct = hits / total * 100 if total > 0 else 0
                        print(f"Chunker: Processed {total} chunks, {hit_pct:.2f}% cache hits")
            row['chunks'] = vectors
            yield row
        # Print final stats
        hit_pct = hits / total * 100 if total > 0 else 0
        print(f"Chunker: Processed {total} chunks, {hit_pct:.2f}% hits")
    return func

def flow(parameters, *_):
    config = parameters['config']
    resource = parameters.get('resource')
    return DF.Flow(
        DF.add_field('chunks', 'array', **{'es:itemType': 'object', 'es:schema': dict(fields=[])}),
        chunker(config, resource=resource),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )

if __name__ == '__main__':
    CACHE_DIR = Path('./.embedcache')
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    data = [
        {'title': 'Sample Title 1', 'text': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.'},
        {'title': 'Sample Title 2', 'text': 'Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'},
        {'title': 'Sample Title 3', 'text': 'Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea.'},
    ]
    config = [
        'first try {title}', 'second try {title}', dict(method='chunk', field='text')
    ]
    parameters = dict(
        config=config,
    )

    DF.Flow(
        data,
        flow(parameters),
        DF.printer(),
    ).process()
