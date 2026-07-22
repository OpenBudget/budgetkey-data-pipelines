import dataflows as DF
from dataflows.helpers.resource_matcher import ResourceMatcher
from pathlib import Path

from datapackage_pipelines_budgetkey.common.cached_openai import embed

# Each chunk becomes a 1536-dim dense_vector, ~20KB of JSON once indexed.
# Cap the chunks per document so a single pathological document can't grow
# past Elasticsearch's http.max_content_length and fail the whole bulk write.
MAX_CHUNKS_PER_DOC = 500


def get_text(c, row):
    if isinstance(c, str):
        yield c.format(**row)
    elif isinstance(c, dict):
        text = row.get(c['field'])
        if text:
            if c.get('method') == 'chunk':
                chunk_size = c.get('chunk_size') or 1000
                chunk_overlap = c.get('chunk_overlap') or 200
                stride = chunk_size - chunk_overlap
                assert stride > 0, \
                    'chunk_overlap (%d) must be smaller than chunk_size (%d)' % (chunk_overlap, chunk_size)
                count = 0
                while len(text) > 0:
                    if count == MAX_CHUNKS_PER_DOC:
                        print(f"Chunker: truncating {c['field']} after {count} chunks, "
                              f"{len(text)} chars left unindexed")
                        break
                    yield text[:chunk_size]
                    text = text[stride:]
                    count += 1

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
