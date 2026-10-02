"""Gemini completions, cached on disk.

All text generation goes through here; embeddings stay on OpenAI (cached_openai.embed).
"""
import os
import json
import time

from google import genai
from google.genai import types

from .ai_cache import hash_text, get_from_cache, save_to_cache

MODEL = os.environ.get('GEMINI_MODEL', 'gemini-3.1-pro-preview')
MODEL_FAST = os.environ.get('GEMINI_MODEL_FAST', 'gemini-3.8-flash')

_client = None


def client():
    global _client
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


def complete(text, structured=False, schema=None, model=None, temperature=None, use_cache=True):
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
    # Join the text parts ourselves: response.text warns about the thought-signature parts thinking models add.
    parts = response.candidates[0].content.parts if response.candidates and response.candidates[0].content else []
    content = ''.join(p.text for p in parts or [] if p.text and not p.thought)
    if structured:
        content = json.loads(content)
    if use_cache:
        save_to_cache(key, content)
    return False, content
