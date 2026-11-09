#!/usr/bin/env python3
"""
=============================================================================
TOOL: serve_dashboard.py
PROJECT: Product Review Data Processing Using Pig & MapReduce
DESCRIPTION:
  Launches a lightweight local HTTP server displaying the interactive
  visual dashboard and working demo with automatic port fallback.
=============================================================================
"""

import http.server
import socketserver
import os
import webbrowser
import sys

CANDIDATE_PORTS = [8000, 5000, 8085, 3000, 9000, 0]
HOST = "127.0.0.1"
WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web")

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

def start_server():
    os.chdir(WEB_DIR)
    httpd = None
    chosen_port = None

    for port in CANDIDATE_PORTS:
        try:
            httpd = socketserver.TCPServer((HOST, port), Handler)
            chosen_port = httpd.server_address[1]
            break
        except OSError:
            continue

    if not httpd:
        print("[!] Could not bind to standard ports. You can directly open the file in your browser:")
        print(f"    File: {os.path.join(WEB_DIR, 'index.html')}")
        print("    Run in PowerShell: start web\\index.html")
        return

    url = f"http://{HOST}:{chosen_port}"
    print("=" * 75)
    print(" AMAZON REVIEW ANALYTICS: INTERACTIVE WORKING DEMO SERVER")
    print("=" * 75)
    print(f"[*] Serving demo at: {url}")
    print("[*] Dashboard opened in your default web browser.")
    print("[*] (Tip: You can also open 'web\\index.html' directly without a server)")
    print("[*] Press Ctrl+C to terminate.")
    print("=" * 75)

    try:
        webbrowser.open(url)
    except Exception:
        pass

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Server stopped.")
    finally:
        httpd.server_close()

if __name__ == "__main__":
    start_server()
