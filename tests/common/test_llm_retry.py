import httpx
import pytest

from datapackage_pipelines_budgetkey.common import llm


class FlakyModels:
    def __init__(self, failures):
        self.failures = list(failures)
        self.calls = 0

    def generate_content(self, **kwargs):
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return 'response'


class FakeClient:
    def __init__(self, models):
        self.models = models


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(llm.time, 'sleep', lambda s: None)


def test_generate_retries_dropped_connections(monkeypatch, no_sleep):
    models = FlakyModels([httpx.ReadError('[Errno 9] Bad file descriptor'), httpx.ConnectError('reset')])
    monkeypatch.setattr(llm, 'client', lambda: FakeClient(models))
    assert llm.generate('hi', None) == 'response'
    assert models.calls == 3


def test_generate_gives_up_after_the_retries(monkeypatch, no_sleep):
    models = FlakyModels([httpx.ReadError('gone')] * 5)
    monkeypatch.setattr(llm, 'client', lambda: FakeClient(models))
    with pytest.raises(httpx.ReadError):
        llm.generate('hi', None, retries=2)
    assert models.calls == 3


def test_generate_does_not_retry_other_errors(monkeypatch, no_sleep):
    models = FlakyModels([ValueError('bad schema')])
    monkeypatch.setattr(llm, 'client', lambda: FakeClient(models))
    with pytest.raises(ValueError):
        llm.generate('hi', None)
    assert models.calls == 1
