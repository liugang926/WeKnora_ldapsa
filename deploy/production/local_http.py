"""Credential-bearing acceptance requests stay on the intended HTTP service."""
import json
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


class LocalHTTPClient:
    def __init__(self, origin, allowed_ports=(18080,)):
        target = urllib.parse.urlsplit(origin)
        if (target.scheme != 'http' or target.hostname not in ('127.0.0.1', '10.106.1.128') or
                target.port not in allowed_ports or target.username is not None or
                target.password is not None or target.path not in ('', '/') or
                target.query or target.fragment):
            raise ValueError('acceptance requires an explicit local production HTTP endpoint')
        self.origin = origin.rstrip('/')
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, path, payload=None, token=None):
        if not path.startswith('/') or path.startswith('//'):
            raise ValueError('acceptance requires a local API path')
        headers = {'Host': 'ai-doc.tianjizn.com'}
        if token:
            headers['Authorization'] = 'Bearer ' + token
        if payload is not None:
            headers['Content-Type'] = 'application/json'
        request = urllib.request.Request(self.origin + path, headers=headers,
            data=None if payload is None else json.dumps(payload).encode())
        try:
            with self.opener.open(request, timeout=30) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()
