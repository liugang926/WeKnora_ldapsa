import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
import threading
import unittest
from unittest import mock

from local_http import LocalHTTPClient


@contextlib.contextmanager
def server(handler):
    instance = ThreadingHTTPServer(('127.0.0.1', 0), handler)
    worker = threading.Thread(target=instance.serve_forever, daemon=True)
    worker.start()
    try:
        yield instance
    finally:
        instance.shutdown()
        instance.server_close()
        worker.join()


class LocalHTTPTest(unittest.TestCase):
    def test_rejects_unexpected_endpoints_and_credentials_in_origin(self):
        for origin in ('http://example.com:18080', 'https://127.0.0.1:18080',
                       'http://127.0.0.1:1', 'http://name@127.0.0.1:18080',
                       'http://127.0.0.1:18080/?redirect=1', 'http://127.0.0.1:18080/#fragment'):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                LocalHTTPClient(origin)

    def test_environment_proxy_is_never_used(self):
        reached = []

        class Direct(BaseHTTPRequestHandler):
            def do_GET(self):
                reached.append(self.path)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'local')

            def log_message(self, *args):
                pass

        with server(Direct) as target, mock.patch.dict(os.environ,
                {'http_proxy': 'http://127.0.0.1:1', 'HTTP_PROXY': 'http://127.0.0.1:1',
                 'no_proxy': '', 'NO_PROXY': ''}):
            port = target.server_port
            client = LocalHTTPClient('http://127.0.0.1:' + str(port), allowed_ports=(port,))
            self.assertEqual(client.request('/health'), (200, b'local'))
        self.assertEqual(reached, ['/health'])

    def test_redirect_does_not_forward_administrator_payload_or_token(self):
        reached = []

        class Redirect(BaseHTTPRequestHandler):
            def do_POST(self):
                reached.append(self.path)
                self.send_response(302)
                self.send_header('Location', self.server.destination)
                self.end_headers()

            def do_GET(self):
                reached.append('unexpected redirected request')
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        with server(Redirect) as target:
            port = target.server_port
            target.destination = 'http://127.0.0.1:' + str(port) + '/unexpected'
            client = LocalHTTPClient('http://127.0.0.1:' + str(port), allowed_ports=(port,))
            self.assertEqual(client.request('/login', {'test': 'fixture'}, 'fixture')[0], 302)
            with self.assertRaises(ValueError):
                client.request('//unexpected')
        self.assertEqual(reached, ['/login'])
