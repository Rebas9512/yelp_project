#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, sys, time, pathlib
from urllib.request import Request, urlopen

MB_BASE   = os.getenv("MB_BASE", "http://localhost:3000").rstrip("/")
MB_EMAIL  = os.getenv("MB_EMAIL", "admin@yelp.local")
MB_PASS   = os.getenv("MB_PASS",  "Metabase!2025")
EXPORT_DIR= pathlib.Path("metabase_export")

def req(path, method="GET", data=None, headers=None, timeout=20):
    url = path if path.startswith("http") else f"{MB_BASE}{path}"
    headers = headers or {}
    if data is not None and not isinstance(data, (bytes, bytearray)):
        headers.setdefault("Content-Type", "application/json")
        data = json.dumps(data).encode("utf-8")
    r = Request(url, data=data, headers=headers, method=method)
    with urlopen(r, timeout=timeout) as resp:
        return resp.getcode(), resp.read()

def login():
    c,b = req("/api/session", "POST", {"username": MB_EMAIL, "password": MB_PASS})
    if c==200:
        sid = json.loads(b).get("id")
        if sid: return sid
    raise RuntimeError("Login failed")

def api(path, sid, method="GET", data=None):
    return req(path, method, data, headers={"X-Metabase-Session": sid})

def ensure_dirs():
    (EXPORT_DIR / "collections").mkdir(parents=True, exist_ok=True)
    (EXPORT_DIR / "cards").mkdir(parents=True, exist_ok=True)
    (EXPORT_DIR / "dashboards").mkdir(parents=True, exist_ok=True)

def save_json(p: pathlib.Path, obj):
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def export_all(sid):
    ensure_dirs()
    manifest = {"collections": [], "cards": [], "dashboards": [], "exported_at": int(time.time())}

    # collections（跳过 root）
    c, b = api("/api/collection", sid)
    cols = json.loads(b)
    for col in cols:
        if col.get("type") == "root":  # 跳过 Root
            continue
        cid = col["id"]
        manifest["collections"].append({"id": cid, "name": col["name"]})
        save_json(EXPORT_DIR / "collections" / f"{cid}.json", col)

    # cards
    c, b = api("/api/card", sid)
    cards = [x for x in json.loads(b) if not x.get("archived")]
    for card in cards:
        cid = card["id"]
        manifest["cards"].append({"id": cid, "name": card.get("name")})
        save_json(EXPORT_DIR / "cards" / f"{cid}.json", card)

    # dashboards
    c, b = api("/api/dashboard", sid)
    dashes = [x for x in json.loads(b) if not x.get("archived")]
    for d in dashes:
        did = d["id"]
        manifest["dashboards"].append({"id": did, "name": d.get("name")})
        # 带上 dashboard cards 详情
        _, db = api(f"/api/dashboard/{did}", sid)
        save_json(EXPORT_DIR / "dashboards" / f"{did}.json", json.loads(db))

    save_json(EXPORT_DIR / "manifest.json", manifest)
    print(f"✅ Exported to {EXPORT_DIR}/")

def main():
    sid = login()
    export_all(sid)

if __name__ == "__main__":
    main()