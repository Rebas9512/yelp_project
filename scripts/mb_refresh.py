#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, urllib.request

BASE = os.getenv("MB_SITE_URL", "http://localhost:3000")
EMAIL = os.getenv("MB_EMAIL", "admin@yelp.local")
PASS = os.getenv("MB_PASS", "Metabase!2025")

def call(path, method="GET", data=None, headers=None):
    headers = headers or {}
    if data is not None and not isinstance(data, (bytes, bytearray)):
        data = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as r:
        return r.read()

def main():
    sid = json.loads(call("/api/session", "POST", {"username": EMAIL, "password": PASS}))["id"]
    hdr = {"X-Metabase-Session": sid}
    dbs = json.loads(call("/api/database", headers=hdr))
    # 兼容两种返回结构：{data:[...]} 或 直接数组
    items = dbs.get("data") if isinstance(dbs, dict) and "data" in dbs else dbs
    db_id = None
    for d in items:
        if "yelp_gold" in d.get("name", ""):
            db_id = d["id"]
            break
    if not db_id:
        print("No DB containing 'yelp_gold' found", file=sys.stderr)
        sys.exit(2)
    call(f"/api/database/{db_id}/sync_schema", "POST", headers=hdr)
    call(f"/api/database/{db_id}/rescan_values", "POST", headers=hdr)
    print(f"Refreshed database id={db_id}")

if __name__ == "__main__":
    main()