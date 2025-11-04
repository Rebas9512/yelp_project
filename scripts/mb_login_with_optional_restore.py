#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, time, webbrowser, pathlib
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError

MB_BASE  = os.getenv("MB_BASE", "http://localhost:3000").rstrip("/")
MB_EMAIL = os.getenv("MB_EMAIL", "admin@yelp.local")
MB_PASS  = os.getenv("MB_PASS",  "Metabase!2025")
MB_DS    = os.getenv("MB_DS_NAME", "yelp_gold")
EXPORT_DIR = os.getenv("MB_EXPORT_DIR", "metabase_export")

def req(url, method="GET", data=None, headers=None, timeout=30):
    headers = headers or {}
    if data is not None and not isinstance(data, (bytes, bytearray)):
        headers.setdefault("Content-Type", "application/json")
        data = json.dumps(data).encode("utf-8")
    r = Request(url, data=data, headers=headers, method=method)
    with urlopen(r, timeout=timeout) as resp:
        return resp.getcode(), resp.read()

def wait_health(base, secs=120):
    print(f"-> Waiting for Metabase health at {base}/api/health ...")
    t0 = time.time()
    while time.time()-t0 < secs:
        try:
            c,b = req(f"{base}/api/health")
            if c==200 and json.loads(b or b"{}").get("status")=="ok":
                print("✅ Metabase ready"); return
        except Exception: pass
        time.sleep(2)
    raise RuntimeError("Metabase not healthy in time")

def login(base, email, password):
    c,b = req(f"{base}/api/session","POST",{"username": email, "password": password})
    j = json.loads(b or b"{}")
    if c==200 and j.get("id"): return j["id"]
    raise RuntimeError(f"login failed {c} {b!r}")

def api(base, path, sid, method="GET", data=None):
    return req(f"{base}{path}", method, data, {"X-Metabase-Session": sid})

# ---------- auto-login ----------
class OneShotHandler(BaseHTTPRequestHandler):
    sid = None
    target = None
    def do_GET(self):
        # 下发 host-only cookie（不写 Domain，保证和请求 host 完全一致）
        self.send_response(302)
        cookie = "metabase.SESSION={}; Path=/; HttpOnly; SameSite=Lax".format(self.sid)
        self.send_header("Set-Cookie", cookie)
        # 跳回 Metabase 首页
        self.send_header("Location", f"{self.target}/")
        self.end_headers()
        self.wfile.write(b"Redirecting to Metabase ...")
        import threading; threading.Thread(target=self.server.shutdown, daemon=True).start()
    def log_message(self, *a, **k): return

def open_browser_with_cookie(session_id, base, port=34567):
    # 与 MB_BASE 的 host 完全一致（比如都是 localhost 或都是 127.0.0.1）
    parsed = urlparse(base)
    cookie_host = parsed.hostname or "localhost"

    OneShotHandler.sid = session_id
    OneShotHandler.target = base

    # 本地小服务同样绑定到 cookie_host
    for p in (port, 0):
        try:
            httpd = HTTPServer((cookie_host, p), OneShotHandler)
            actual = httpd.server_address[1]
            break
        except OSError:
            continue

    url = f"http://{cookie_host}:{actual}/"
    print(f"-> Opening browser: {url}")
    try:
        webbrowser.open(url)
    except Exception:
        print(f"(open manually): {url}")
    httpd.serve_forever()

# ---------- helpers ----------
def get_databases(base, sid):
    c,b = api(base,"/api/database",sid)
    js = json.loads(b or b"[]"); return js.get("data", js)

def get_collections(base, sid):
    c,b = api(base,"/api/collection",sid)
    return json.loads(b or b"[]")

def ensure_collection(base, sid, name, parent_id=None):
    for cobj in get_collections(base, sid):
        if cobj.get("name")==name and not cobj.get("archived"):
            return cobj["id"]
    payload={"name": name}
    if parent_id: payload["parent_id"]=parent_id
    c,b = api(base,"/api/collection",sid,"POST",payload)
    return json.loads(b or b"{}")["id"]

def create_card(base, sid, card_json, target_db_id, collection_id):
    for k in ["id","collection","creator","updated_at","created_at","public_uuid","dataset"]:
        card_json.pop(k, None)
    if "database_id" in card_json and card_json["database_id"]:
        card_json["database_id"] = target_db_id
    dq = card_json.get("dataset_query")
    if isinstance(dq, dict) and "database" in dq:
        dq["database"] = target_db_id
    card_json["collection_id"] = collection_id
    c,b = api(base,"/api/card",sid,"POST",card_json)
    return json.loads(b or b"{}")["id"]

def create_dashboard_skeleton(base, sid, dash_json, target_collection_id, attachment_notes):
    for k in ["id","creator","updated_at","created_at","public_uuid","dashcards","ordered_cards"]:
        dash_json.pop(k, None)
    name = dash_json.get("name") or "Imported Dashboard"
    base_desc = dash_json.get("description") or ""
    hint = "\n\n[Imported]\n" + "\n".join(attachment_notes) if attachment_notes else ""
    payload = {"name": name, "description": base_desc + hint, "collection_id": target_collection_id}
    c,b = api(base, "/api/dashboard", sid, "POST", payload)
    return json.loads(b or b"{}")["id"]

def restore_if_present(base, sid):
    root = pathlib.Path(EXPORT_DIR)
    if not root.exists():
        print("-> No saved Metabase project found; skip restore."); return

    # 1) 找目标 DB
    dbs = get_databases(base, sid)
    target_db_id = None
    for d in dbs:
        if d.get("name")==MB_DS: target_db_id = d["id"]; break
    if not target_db_id:
        for d in dbs:
            if (d.get("engine") or "").lower()!="h2":
                target_db_id = d["id"]; break
    if not target_db_id:
        raise RuntimeError("No target database in Metabase to bind cards.")

    # 2) 目标集合
    coll_root_id = ensure_collection(base, sid, "Imported")

    # 3) 先导入卡片
    cards_dir = root/"cards"
    card_name_by_old = {}
    id_map = {}
    if cards_dir.exists():
        for p in sorted(cards_dir.glob("*.json")):
            card = json.loads(p.read_text(encoding="utf-8"))
            old_id = card.get("id"); name = card.get("name","(unnamed)")
            new_id = create_card(base, sid, card, target_db_id, coll_root_id)
            id_map[old_id] = new_id; card_name_by_old[old_id]=name
            print(f"   - card {old_id}->{new_id} [{name}]")

    # 4) 再创建“空看板 + 备注清单”
    dashes_dir = root/"dashboards"
    if dashes_dir.exists():
        for p in sorted(dashes_dir.glob("*.json")):
            dash = json.loads(p.read_text(encoding="utf-8"))
            # 收集建议挂载卡片的文字提示
            notes = []
            for dc in dash.get("dashcards", []):
                old_id = (dc.get("card", {}) or {}).get("id") or dc.get("card_id")
                if not old_id: continue
                name = card_name_by_old.get(old_id, f"old_card_{old_id}")
                new_id = id_map.get(old_id)
                notes.append(f"- Please add card “{name}” (new id: {new_id}) manually.")
            new_dash_id = create_dashboard_skeleton(base, sid, dash, coll_root_id, notes)
            print(f"   - dashboard -> {new_dash_id} [{dash.get('name','Dashboard')}]")

    print("✅ Restore completed (cards imported; dashboards created without attachments due to API removal)")

def main():
    wait_health(MB_BASE)
    print(f"-> Logging in to {MB_BASE} as {MB_EMAIL} ...")
    sid = login(MB_BASE, MB_EMAIL, MB_PASS)
    print("   session:", sid)
    # optional restore
    if pathlib.Path(EXPORT_DIR).exists():
        print("-> Found saved Metabase project. Restoring ...")
        restore_if_present(MB_BASE, sid)
    else:
        print("-> No project to restore; skipping.")
    # open browser
    parsed = urlparse(MB_BASE); host = "127.0.0.1"
    # open cookie server
    class HS(OneShotHandler): pass
    open_browser_with_cookie(sid, MB_BASE)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Failed:", e, file=sys.stderr); sys.exit(1)