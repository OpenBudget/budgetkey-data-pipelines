import dataflows as DF
from openai import OpenAI
from hashlib import sha256
from pathlib import Path
import pickle
import os

CACHE_DIR = Path('/var/embedcache')

def hash_text(text):
    return sha256(text.encode('utf-8')).hexdigest()

def path_for_hash(hash):
    base = CACHE_DIR / hash[0:2] / hash[2:4] / hash[4:6]
    base.mkdir(parents=True, exist_ok=True)
    return base / hash[6:]

def get_from_cache(hash):
    path = path_for_hash(hash)
    if path.exists():
        with path.open('rb') as f:
            try:
                return pickle.load(f)
            except EOFError:
                # Handle the case where the file is empty or corrupted
                pass
    return None

def save_to_cache(hash, data):
    path = path_for_hash(hash)
    with path.open('wb') as f:
        pickle.dump(data, f)

def embed(client: OpenAI, text):
    hash = hash_text(text)
    cached = get_from_cache(hash)
    if cached:
        return True, cached

    embedding = client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    )
    embedding = embedding.data[0].embedding
    save_to_cache(hash, embedding)
    
    return False, embedding

def get_text(c, row):
    if isinstance(c, str):
        yield c.format(**row)
    elif isinstance(c, dict):
        text = row.get(c['field'])
        if c.get('method') == 'chunk':
            chunk_size = c.get('chunk_size') or 512
            chunk_overlap = c.get('chunk_overlap') or 0
            while len(text) > 0:
                yield text[:chunk_size]
                text = text[chunk_size - chunk_overlap:]

def chunker(config):
    client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])
    def func(rows):
        hits = 0
        total = 0
        for row in rows:
            vectors = []
            for c in config:
                for chunk in get_text(c, row):
                    hit, embedding = embed(client, chunk)
                    vectors.append(dict(embeddings=embedding))
                    if hit:
                        hits += 1
                    total += 1
                    if total % 10000 == 0:
                        hit_pct = hits / total * 100
                        print(f"Chunker: Processed {total} chunks, {hit_pct:.2f}% cache hits")
            row['chunks'] = vectors
            yield row
        hit_pct = hits / total * 100
        print(f"Chunker: Processed {total} chunks, {hit_pct:.2f}% hits")
    return func

def flow(parameters, *_):
    config = parameters['config']
    return DF.Flow(
        DF.add_field('chunks', 'array'),
        chunker(config),   
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
