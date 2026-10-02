import os
from openai import OpenAI

from .ai_cache import hash_text, get_from_cache, save_to_cache

_client = None


def client():
    global _client
    if _client is None:
        _client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])
    return _client


def embed(text):
    hash = hash_text(text)
    cached = get_from_cache(hash)
    if cached:
        return True, cached

    embedding = client().embeddings.create(
        model="text-embedding-3-small",
        input=text
    )
    embedding = embedding.data[0].embedding
    save_to_cache(hash, embedding)

    return False, embedding


def embed_many(texts, batch_size=256):
    """Like embed(), for a list: cache misses are sent in batches. Returns a list of embeddings."""
    hashes = [hash_text(t) for t in texts]
    result = [get_from_cache(h) for h in hashes]
    missing = [i for i, r in enumerate(result) if not r]
    for start in range(0, len(missing), batch_size):
        batch = missing[start:start + batch_size]
        response = client().embeddings.create(
            model="text-embedding-3-small",
            input=[texts[i] for i in batch]
        )
        for i, item in zip(batch, response.data):
            result[i] = item.embedding
            save_to_cache(hashes[i], item.embedding)
    return result
