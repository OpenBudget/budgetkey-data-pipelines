import os
import json
from hashlib import sha256
import pickle
from pathlib import Path
from openai import OpenAI

CACHE_DIR = Path('/var/ai-cache')

client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

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

def embed(text):
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

def complete(text, structured=False):
    hash = hash_text(text)
    cached = get_from_cache(hash)
    if cached:
        print('CACHE HIT', hash, cached)
        return True, cached

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": [
                    { "type": "text", "text": text },
                ],
            }
        ],
        response_format=dict(type='json_object') if structured else dict(type='text'),
    )

    content = completion.choices[0].message.content
    
    if structured:
        content = json.loads(content)
    save_to_cache(hash, content)
    return False, content
