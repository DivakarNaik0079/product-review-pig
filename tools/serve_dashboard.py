#!/usr/bin/env python3
"""
=============================================================================
TOOL: serve_dashboard.py
PROJECT: Product Review Data Processing Using Pig & MapReduce
DESCRIPTION:
  Launches a lightweight local HTTP server displaying the interactive
  visual dashboard and working demo at: http://localhost:8080
=============================================================================
"""

import http.server
import socketserver
import os
import webbrowser
import sys

PORT = 8080
WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web")

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

def start_server():
    os.chdir(WEB_DIR)
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        url = f"http://localhost:{PORT}"
        print("=" * 75)
        print(" AMAZON REVIEW ANALYTICS: INTERACTIVE WORKING DEMO SERVER")
        print("=" * 75)
        print(f"[*] Serving demo at: {url}")
        print("[*] Open this link in any browser to inspect charts, Pig tasks, and MapReduce jobs.")
        print("[*] Press Ctrl+C to terminate the server.")
        print("=" * 75)
        try:
            webbrowser.open(url)
        except Exception:
            pass
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n[!] Server stopped.")

if __name__ == "__main__":
    start_server()
