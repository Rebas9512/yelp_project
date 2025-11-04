#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import time
import threading
import webbrowser
import pathlib
import copy
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError

MB_BASE    = os.getenv("MB_BASE", "http://localhost:3000").rstrip("/")
MB_EMAIL   = os.getenv("MB_EMAIL", "admin@yelp.local")
MB_PASS    = os.getenv("MB_PASS", "Metabase!2025")
MB_DS_NAME = os.getenv("MB_DS_NAME", "yelp_gold")  # 导入时指向的数据源名称
EXPORT_DIR = pathlib.Path("metabase_export")

# -------------------- HTTP helpers --------------------
def _req(url, method="GET", data=None, headers=None, timeout=30):
    headers = headers or {}
    if data is not None and not isinstance(data, (bytes, bytearray)):
        headers.setdefault("Content-Type", "application/json")
        data = json.dumps(data).encode("utf-8")
    r = Request(url, data=data, headers=headers, method=method)
    with urlopen(r, timeout=timeout) as resp:
        return resp.getcode(), resp.read()

def req_api(path, method="GET", data=None, headers=None, timeout=30):
    url = path if path.startswith("http") else f"{MB_BASE}{path}"
    return _req(url, method, data, headers, timeout)

def wait_health(max_secs=120):
    start = time.time()
    print(f"-> Waiting for Metabase health at {MB_BASE}/api/health ...")
    while time.time() - start < max_secs:
        try:
            code, body = req_api("/api/health")
            if code == 200 and json.loads(body or b"{}").get("status") == "ok":
                print("✅ Metabase ready")
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("Metabase not healthy in time")

# -------------------- Auth / setup --------------------
def session_properties():
    try:
        _, b = req_api("/api/session/properties")
        j = json.loads(b or b"{}")
        return j.get("setup_token") or j.get("setup-token")
    except Exception:
        return None

def login_get_session_id(base, email, password) -> str:
    url = f"{base}/api/session"
    data = {"username": email, "password": password}
    code, body = _req(url, "POST", data)
    if code == 200:
        sid = json.loads(body).get("id")
        if sid:
            return sid
    raise RuntimeError(f"Login failed, response: {body!r}")

def setup_instance_if_needed() -> str:
    """如果未初始化则调用 /api/setup 完成初始化，返回 session id"""
    try:
        return login_get_session_id(MB_BASE, MB_EMAIL, MB_PASS)
    except HTTPError as e:
        if e.code != 401:
            raise
    except RuntimeError:
        # 401/失败时，尝试 setup
        pass
    token = session_properties()
    if not token:
        # 可能已经初始化，只是账号密码不对
        raise RuntimeError("Cannot login and no setup token; check MB_EMAIL/MB_PASS")
    payloads = [
        {"token": token,
         "user": {"first_name": "Admin", "last_name": "User", "email": MB_EMAIL, "password": MB_PASS},
         "site_name": "Yelp BI", "prefs": {"allow_tracking": False}},
        {"token": token,
         "user": {"first_name": "Admin", "last_name": "User", "email": MB_EMAIL, "password": MB_PASS},
         "prefs": {"site_name": "Yelp BI", "allow_tracking": False}},
    ]
    last = None
    for p in payloads:
        try:
            code, b = req_api("/api/setup", "POST", p)
            j = json.loads(b or b"{}")
            if code == 200 and j.get("id"):
                return j["id"]
            last = (code, b)
        except Exception as e:
            last = e
    raise RuntimeError(f"/api/setup failed: {last}")

def api(path, sid, method="GET", data=None):
    return req_api(path, method, data, headers={"X-Metabase-Session": sid})

# -------------------- Import (optional) --------------------
def get_db_id_by_name(sid, name):
    _, b = api("/api/database", sid)
    lst = json.loads(b)
    dbs = lst.get("data", lst)
    for d in dbs:
        if d.get("name") == name:
            return d["id"]
    raise RuntimeError(f"Datasource '{name}' not found")

def ensure_collection_by_name(sid, name, parent_id=None):
    _, b = api("/api/collection", sid)
    cols = json.loads(b)
    for c in cols:
        if c.get("name") == name and not c.get("archived"):
            return c["id"]
    payload = {"name": name}
    if parent_id:
        payload["parent_id"] = parent_id
    _, rb = api("/api/collection", sid, "POST", payload)
    return json.loads(rb)["id"]

def upsert_card(sid, card_obj, db_id, target_collection_id):
    obj = copy.deepcopy(card_obj)
    obj.pop("id", None)
    obj["collection_id"] = target_collection_id
    # 规范化 database 指向
    if isinstance(obj.get("dataset_query"), dict):
        dq = obj["dataset_query"]
        if "database" in dq:
            dq["database"] = db_id
    if "database_id" in obj:
        obj["database_id"] = db_id

    # 以名称幂等
    _, b = api("/api/card", sid)
    cards = json.loads(b)
    exists = next((x for x in cards if x.get("name") == obj.get("name") and not x.get("archived")), None)
    if exists:
        cid = exists["id"]
        _, rb = api(f"/api/card/{cid}", sid, "PUT", obj)
        return json.loads(rb)["id"]
    else:
        _, rb = api("/api/card", sid, "POST", obj)
        return json.loads(rb)["id"]

def upsert_dashboard(sid, dash_obj, target_collection_id, name_to_card_id):
    obj = copy.deepcopy(dash_obj)
    obj.pop("id", None)
    obj["collection_id"] = target_collection_id
    dash_name = obj.get("name")

    # 以名称幂等创建/更新 Dashboard
    _, b = api("/api/dashboard", sid)
    dashes = json.loads(b)
    exists = next((x for x in dashes if x.get("name") == dash_name and not x.get("archived")), None)
    if exists:
        did = exists["id"]
        meta = {k: obj.get(k) for k in ["name", "description", "collection_id"]}
        api(f"/api/dashboard/{did}", sid, "PUT", meta)
    else:
        _, rb = api("/api/dashboard", sid, "POST", {"name": dash_name, "collection_id": target_collection_id})
        did = json.loads(rb)["id"]

    # 恢复布局（含文本卡）
    _, cur = api(f"/api/dashboard/{did}", sid)
    ordered_cards = obj.get("ordered_cards") or []
    for oc in ordered_cards:
        viz = oc.get("visualization_settings") or {}
        # 文本卡（markdown）
        if viz.get("markdown"):
            payload = {
                "dashboard_id": did,
                "parameter_mappings": oc.get("parameter_mappings") or [],
                "visualization_settings": viz,
                "col": oc.get("col", 0), "row": oc.get("row", 0),
                "size_x": oc.get("size_x", 4), "size_y": oc.get("size_y", 3),
                "cardId": None, "text": viz["markdown"], "series": []
            }
            api(f"/api/dashboard/{did}/cards", sid, "POST", payload)
            continue

        # 普通卡片
        card_ref = oc.get("card") or {}
        card_name = card_ref.get("name")
        if not card_name:
            continue
        cid = name_to_card_id.get(card_name)
        if not cid:
            continue
        payload = {
            "dashboard_id": did,
            "cardId": cid,
            "col": oc.get("col", 0), "row": oc.get("row", 0),
            "size_x": oc.get("size_x", 4), "size_y": oc.get("size_y", 3),
            "parameter_mappings": oc.get("parameter_mappings") or [],
            "visualization_settings": viz,
            "series": oc.get("series") or []
        }
        api(f"/api/dashboard/{did}/cards", sid, "POST", payload)
    return did

def restore_if_export_exists(sid):
    if not (EXPORT_DIR / "manifest.json").exists():
        print("-> No saved Metabase project. Skip restore.")
        return
    print("-> Found saved Metabase project. Restoring ...")
    manifest = json.loads((EXPORT_DIR / "manifest.json").read_text(encoding="utf-8"))
    # 目标数据源
    db_id = get_db_id_by_name(sid, MB_DS_NAME)

    # 集合
    exported_cols = manifest.get("collections", [])
    col_name_map = {}
    for cinfo in exported_cols:
        col_obj = json.loads((EXPORT_DIR / "collections" / f"{cinfo['id']}.json").read_text(encoding="utf-8"))
        new_id = ensure_collection_by_name(sid, col_obj["name"])
        col_name_map[col_obj["name"]] = new_id

    # 默认集合
    default_col_id = ensure_collection_by_name(sid, "Yelp BI")

    # 卡片
    name_to_card_id = {}
    for cinfo in manifest.get("cards", []):
        card = json.loads((EXPORT_DIR / "cards" / f"{cinfo['id']}.json").read_text(encoding="utf-8"))
        col_id = default_col_id
        try:
            col_name = (card.get("collection") or {}).get("name")
            if col_name and col_name in col_name_map:
                col_id = col_name_map[col_name]
        except Exception:
            pass
        new_cid = upsert_card(sid, card, db_id, col_id)
        name_to_card_id[card.get("name")] = new_cid

    # 仪表盘
    for dinfo in manifest.get("dashboards", []):
        dash = json.loads((EXPORT_DIR / "dashboards" / f"{dinfo['id']}.json").read_text(encoding="utf-8"))
        col_id = default_col_id
        try:
            col_name = (dash.get("collection") or {}).get("name")
            if col_name and col_name in col_name_map:
                col_id = col_name_map[col_name]
        except Exception:
            pass
        upsert_dashboard(sid, dash, col_id, name_to_card_id)

    print("✅ Restore done.")

# -------------------- One-shot cookie server --------------------
class OneShotHandler(BaseHTTPRequestHandler):
    session_id = None
    target_base = None

    def do_GET(self):
        # 发放 host-only cookie（不带 Domain，兼容 localhost）
        self.send_response(302)
        cookie = (
            f"metabase.SESSION={self.session_id}; "
            f"Path=/; HttpOnly; SameSite=Lax"
        )
        self.send_header("Set-Cookie", cookie)
        self.send_header("Location", f"{self.target_base}/")
        self.end_headers()
        self.wfile.write(b"Redirecting to Metabase ...")
        threading.Thread(target=self.server.shutdown, daemon=True).start()

    def log_message(self, fmt, *args):
        return

def run_server_once(session_id: str, target_base: str, port: int = 34567):
    # 绑定到与 MB_BASE 同一个 host，发 host-only cookie
    parsed = urlparse(target_base)
    cookie_host = parsed.hostname or "localhost"

    handler = OneShotHandler
    handler.session_id = session_id
    handler.target_base = target_base

    for p in (port, 0):
        try:
            httpd = HTTPServer((cookie_host, p), handler)
            actual_port = httpd.server_address[1]
            break
        except OSError:
            continue
    else:
        print("❌ Failed to bind a local port", file=sys.stderr)
        sys.exit(1)

    url = f"http://{cookie_host}:{actual_port}/"
    print(f"-> Opening browser: {url}")
    try:
        webbrowser.open(url)
    except Exception:
        print(f"(open this URL in your browser manually): {url}")
    httpd.serve_forever()

# -------------------- Main --------------------
def main():
    parsed = urlparse(MB_BASE)
    try:
        wait_health()
        # 登录 / 首次初始化
        try:
            sid = login_get_session_id(MB_BASE, MB_EMAIL, MB_PASS)
            print(f"-> Logging in to {MB_BASE} as {MB_EMAIL} ...")
        except Exception:
            print("-> Instance appears uninitialized; running /api/setup ...")
            sid = setup_instance_if_needed()
        print("   session:", sid)

        # 如有保存项目先导入
        restore_if_export_exists(sid)

        # 自动登录打开浏览器
        run_server_once(sid, MB_BASE)
    except Exception as e:
        print("❌ Failed:", e, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()