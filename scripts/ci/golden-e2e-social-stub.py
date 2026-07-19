#!/usr/bin/env python3
"""Minimal local Social API contract stub for Golden E2E delivery success."""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 - http.server API name
        length = int(self.headers.get("Content-Length", "0"))
        json.loads(self.rfile.read(length) or b"{}")
        body = json.dumps({"social_delivery_id": "golden-e2e-social-delivery", "status": "accepted"}).encode()
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
