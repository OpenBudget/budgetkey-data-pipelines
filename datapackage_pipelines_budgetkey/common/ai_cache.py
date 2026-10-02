import os
import pickle
from hashlib import sha256
from pathlib import Path

CACHE_DIR = Path(os.environ.get('AI_CACHE_DIR', '/var/ai-cache'))


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
