import os
import re
import threading
from openai import BadRequestError, OpenAI

from .ai_cache import hash_text, get_from_cache, save_to_cache

_client = None
_client_lock = threading.Lock()


def client():
    global _client
    with _client_lock:
        if _client is None:
            _client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])
    return _client


MIN_TRUNCATED_CHARS = 500


def _too_long(error):
    # The single-input endpoint says "maximum context length", batches say "maximum input length".
    return isinstance(error, BadRequestError) and re.search(r'maximum (context|input) length', str(error)) is not None


def _embed_uncached(text):
    """Embeds text, halving it while the model rejects it as too long (8192 tokens for text-embedding-3-small)."""
    while True:
        try:
            response = client().embeddings.create(model="text-embedding-3-small", input=text)
            return response.data[0].embedding
        except BadRequestError as e:
            if not _too_long(e) or len(text) < MIN_TRUNCATED_CHARS:
                raise
            text = text[:len(text) // 2]


def embed(text):
    hash = hash_text(text)
    cached = get_from_cache(hash)
    if cached:
        return True, cached

    # Cached under the original text, so a truncated embedding is computed only once.
    embedding = _embed_uncached(text)
    save_to_cache(hash, embedding)

    return False, embedding


def embed_many(texts, batch_size=256):
    """Like embed(), for a list: cache misses are sent in batches. Returns a list of embeddings."""
    hashes = [hash_text(t) for t in texts]
    result = [get_from_cache(h) for h in hashes]
    missing = [i for i, r in enumerate(result) if not r]
    for start in range(0, len(missing), batch_size):
        batch = missing[start:start + batch_size]
        try:
            response = client().embeddings.create(
                model="text-embedding-3-small",
                input=[texts[i] for i in batch]
            )
            embeddings = [item.embedding for item in response.data]
        except BadRequestError as e:
            if not _too_long(e):
                raise
            # One input is too long for the model: embed this batch one by one, truncating as needed.
            embeddings = [_embed_uncached(texts[i]) for i in batch]
        for i, embedding in zip(batch, embeddings):
            result[i] = embedding
            save_to_cache(hashes[i], embedding)
    return result
