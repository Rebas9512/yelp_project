#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import threading
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from urllib.request import Request, urlopen

MB_BASE = os.getenv("MB_BASE", "http://localhost:3000").rstrip("/")
MB_EMAIL = os.getenv("MB_EMAIL", "admin@yelp.local")
MB_PASS = os.getenv("MB_PASS", "Metabase!2025")

def login_get_session_id(base, email, password) -> str:
    url = f"{base}/api/session"
    data = json.dumps({"username": email, "password": password}).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req) as resp:
        body = resp.read()
        obj = json.loads(body)
        sid = obj.get("id")
        if not sid:
            raise RuntimeError(f"Login failed, response: {body!r}")
        return sid

class OneShotHandler(BaseHTTPRequestHandler):
    session_id = None
    target_base = None
    cookie_domain = None

    def do_GET(self):
        # 下发 cookie 并 302 跳转到 Metabase
        self.send_response(302)
        cookie = (
            f"metabase.SESSION={self.session_id}; "
            f"Path=/; Domain={self.cookie_domain}; HttpOnly; SameSite=Lax"
        )
        self.send_header("Set-Cookie", cookie)
        self.send_header("Location", f"{self.target_base}/")
        self.end_headers()
        self.wfile.write(b"Redirecting to Metabase ...")
        # 完成一次请求后关闭服务器
        threading.Thread(target=self.server.shutdown, daemon=True).start()

    # 降低日志噪音
    def log_message(self, fmt, *args):
        return

def run_server_once(session_id: str, target_base: str, cookie_domain: str, port: int = 34567):
    handler = OneShotHandler
    handler.session_id = session_id
    handler.target_base = target_base
    handler.cookie_domain = cookie_domain

    # 尝试固定端口，失败就用系统随机端口
    for p in (port, 0):
        try:
            httpd = HTTPServer(("127.0.0.1", p), handler)
            actual_port = httpd.server_address[1]
            break
        except OSError:
            continue
    else:
        print("❌ Failed to bind a local port", file=sys.stderr)
        sys.exit(1)

    url = f"http://127.0.0.1:{actual_port}/"
    print(f"-> Opening browser: {url}")
    try:
        webbrowser.open(url)
    except Exception:
        print(f"(open this URL in your browser manually): {url}")

    httpd.serve_forever()

def main():
    parsed = urlparse(MB_BASE)
    host = parsed.hostname or "localhost"
    try:
        print(f"-> Logging in to {MB_BASE} as {MB_EMAIL} ...")
        sid = login_get_session_id(MB_BASE, MB_EMAIL, MB_PASS)
        print("   session:", sid)
        run_server_once(sid, MB_BASE, host)
    except Exception as e:
        print("❌ Failed:", e, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()