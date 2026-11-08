import os
import pickle
import threading
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
            except Exception:
                # An empty or corrupted entry (e.g. one cut off by a crash) is a cache miss
                pass
    return None


def save_to_cache(hash, data):
    """Written to a temporary file and renamed into place, so a reader in another thread or process never sees a
    partly written entry."""
    path = path_for_hash(hash)
    tmp = path.with_name('%s.%d.%d.tmp' % (path.name, os.getpid(), threading.get_ident()))
    with tmp.open('wb') as f:
        pickle.dump(data, f)
    os.replace(tmp, path)
