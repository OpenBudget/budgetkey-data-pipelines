"""Minimal client for the BudgetKey MCP server (streamable HTTP, JSON-RPC).

Only what the page agent needs: the server's instructions, its tool list, and tool calls.
Written over `requests` rather than the `mcp` SDK so it runs on the pipelines' Python.
"""
import json
import os
import threading

import requests

MCP_URL = os.environ.get('BUDGETKEY_MCP_URL', 'https://next.obudget.org/mcp')
PROTOCOL_VERSION = '2025-03-26'
TIMEOUT = 120


class MCPError(RuntimeError):
    pass


class MCPClient:

    def __init__(self, url=MCP_URL):
        self.url = url
        self.session_id = None
        self.instructions = None
        self._id = 0
        self._lock = threading.Lock()
        self._http = requests.Session()

    def _headers(self):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/event-stream',
            'mcp-protocol-version': PROTOCOL_VERSION,
        }
        if self.session_id:
            headers['mcp-session-id'] = self.session_id
        return headers

    def _next_id(self):
        with self._lock:
            self._id += 1
            return self._id

    @staticmethod
    def _parse(response):
        """A response is either plain JSON or an SSE stream; return the JSON-RPC message with a result/error."""
        # The server sends text/event-stream without a charset, which requests would decode as Latin-1.
        text = response.content.decode('utf-8')
        if response.headers.get('content-type', '').startswith('text/event-stream'):
            for line in text.splitlines():
                if line.startswith('data:'):
                    message = json.loads(line[5:].strip())
                    if 'result' in message or 'error' in message:
                        return message
            raise MCPError('No result in event stream')
        return json.loads(text)

    def _post(self, payload):
        response = self._http.post(self.url, headers=self._headers(), json=payload, timeout=TIMEOUT)
        if response.status_code == 404 and self.session_id:
            # Session expired on the server - start a new one and retry once.
            self.session_id = None
            self.initialize()
            response = self._http.post(self.url, headers=self._headers(), json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        return response

    def request(self, method, params=None):
        payload = dict(jsonrpc='2.0', id=self._next_id(), method=method)
        if params is not None:
            payload['params'] = params
        message = self._parse(self._post(payload))
        if 'error' in message:
            raise MCPError(message['error'])
        return message['result']

    def initialize(self):
        payload = dict(jsonrpc='2.0', id=self._next_id(), method='initialize', params=dict(
            protocolVersion=PROTOCOL_VERSION, capabilities={},
            clientInfo=dict(name='budgetkey-analysis', version='1'),
        ))
        response = self._http.post(self.url, headers=self._headers(), json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        self.session_id = response.headers.get('mcp-session-id')
        result = self._parse(response)['result']
        self.instructions = result.get('instructions')
        self._http.post(self.url, headers=self._headers(), timeout=TIMEOUT,
                        json=dict(jsonrpc='2.0', method='notifications/initialized'))
        return result

    def list_tools(self):
        return self.request('tools/list')['tools']

    def call_tool(self, name, arguments):
        """Returns the tool's structured result (a dict). Tool-level errors come back as {'error': ...}."""
        result = self.request('tools/call', dict(name=name, arguments=arguments))
        if result.get('structuredContent') is not None:
            content = result['structuredContent']
        else:
            text = ''.join(c.get('text', '') for c in result.get('content', []) if c.get('type') == 'text')
            try:
                content = json.loads(text)
            except ValueError:
                content = dict(text=text)
        if result.get('isError') and 'error' not in content:
            content = dict(error=content)
        if not isinstance(content, dict):
            content = dict(result=content)
        return content
