"""Gemini completions, cached on disk.

All text generation goes through here; embeddings stay on OpenAI (cached_openai.embed).
"""
import os
import json
import threading
import time

from google import genai
from google.genai import types

from .ai_cache import hash_text, get_from_cache, save_to_cache

MODEL = os.environ.get('GEMINI_MODEL', 'gemini-3.1-pro-preview')
MODEL_FAST = os.environ.get('GEMINI_MODEL_FAST', 'gemini-3.8-flash')

_client = None
_client_lock = threading.Lock()


def client():
    # Locked: threads racing to create the client would each build one, and the losers' destructors close
    # the HTTP connection the winner still uses ("Cannot send a request, as the client has been closed").
    global _client
    with _client_lock:
        if _client is None:
            _client = genai.Client(api_key=os.environ['GEMINI_API_KEY'])
    return _client


def _cache_key(text, structured, schema, model, temperature):
    # Plain calls keep the bare-text key, so answers cached before the move to Gemini
    # (gpt-4.1, same prompts) are still hits and don't get re-billed.
    if schema is None and model is None and temperature is None:
        return hash_text(text)
    return hash_text(json.dumps(
        dict(text=text, structured=structured, schema=schema, model=model, temperature=temperature),
        sort_keys=True, ensure_ascii=False
    ))


class Usage:
    """Accumulates token usage per model across calls."""

    FIELDS = ('prompt_token_count', 'cached_content_token_count', 'candidates_token_count', 'thoughts_token_count')

    def __init__(self):
        self.by_model = {}
        self._lock = threading.Lock()

    def add(self, model, response):
        meta = getattr(response, 'usage_metadata', None)
        if meta is None:
            return
        with self._lock:
            entry = self.by_model.setdefault(model, dict({f: 0 for f in self.FIELDS}, calls=0))
            entry['calls'] += 1
            for f in self.FIELDS:
                entry[f] += getattr(meta, f, None) or 0

    def merge(self, other):
        for model, entry in other.by_model.items():
            mine = self.by_model.setdefault(model, dict({f: 0 for f in self.FIELDS}, calls=0))
            for k, v in entry.items():
                mine[k] += v


def apply_thinking(config, level):
    """Sets the Gemini 3 thinking level ('low'/'medium'/'high') on a GenerateContentConfig; None keeps the model default.

    The last SDK release supporting Python 3.9 (which the pipelines run on) predates the thinking_level
    field, so there it is passed through as a raw request field instead.
    """
    if not level:
        return config
    if 'thinking_level' in types.ThinkingConfig.model_fields:
        config.thinking_config = types.ThinkingConfig(thinking_level=level.upper())
    else:
        config.http_options = types.HttpOptions(
            extra_body={'generationConfig': {'thinkingConfig': {'thinkingLevel': level.upper()}}})
    return config


class PromptCache:
    """An explicit Gemini context cache holding a fixed system prompt + tool declarations.

    Shared by every conversation that uses the same prefix (e.g. all pages in a run), so that
    prefix is always billed at the cached rate instead of relying on implicit caching.
    """

    def __init__(self, model, system_instruction, tools, ttl_seconds=7200):
        self.model = model
        self.system_instruction = system_instruction
        self.tools = tools
        self.ttl_seconds = ttl_seconds
        self._name = None
        self._lock = threading.Lock()

    def name(self, refresh=False):
        with self._lock:
            if self._name is None or refresh:
                cache = client().caches.create(model=self.model, config=types.CreateCachedContentConfig(
                    system_instruction=self.system_instruction, tools=self.tools,
                    ttl='%ds' % self.ttl_seconds,
                ))
                self._name = cache.name
                print('CREATED prompt cache {} ({} tokens)'.format(cache.name, cache.usage_metadata.total_token_count))
            return self._name

    def delete(self):
        with self._lock:
            if self._name:
                try:
                    client().caches.delete(name=self._name)
                except genai.errors.APIError as e:
                    print('Failed to delete prompt cache {}: {}'.format(self._name, e))
                self._name = None


def generate(contents, config, model=None, retries=4):
    """Raw generate_content with retry on transient errors (429/5xx)."""
    delay = 5
    for attempt in range(retries + 1):
        try:
            return client().models.generate_content(model=model or MODEL, contents=contents, config=config)
        except genai.errors.APIError as e:
            if attempt == retries or e.code not in (429, 500, 502, 503, 504):
                raise
            print('GEMINI ERROR {}, retrying in {}s'.format(e.code, delay))
            time.sleep(delay)
            delay *= 2


def complete(text, structured=False, schema=None, model=None, temperature=None, use_cache=True, usage=None):
    """Returns (cache_hit, content). With structured=True (or a JSON schema) content is parsed JSON."""
    structured = structured or schema is not None
    key = _cache_key(text, structured, schema, model, temperature)
    if use_cache:
        cached = get_from_cache(key)
        if cached:
            return True, cached

    config = types.GenerateContentConfig(temperature=temperature)
    if structured:
        config.response_mime_type = 'application/json'
        if schema is not None:
            config.response_json_schema = schema
    response = generate(text, config, model=model)
    if usage is not None:
        usage.add(model or MODEL, response)
    # Join the text parts ourselves: response.text warns about the thought-signature parts thinking models add.
    parts = response.candidates[0].content.parts if response.candidates and response.candidates[0].content else []
    content = ''.join(p.text for p in parts or [] if p.text and not p.thought)
    if structured:
        content = json.loads(content)
    if use_cache:
        save_to_cache(key, content)
    return False, content
