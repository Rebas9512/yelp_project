#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, sys, pathlib, copy
from urllib.request import Request, urlopen

MB_BASE    = os.getenv("MB_BASE", "http://localhost:3000").rstrip("/")
MB_EMAIL   = os.getenv("MB_EMAIL", "admin@yelp.local")
MB_PASS    = os.getenv("MB_PASS",  "Metabase!2025")
MB_DS_NAME = os.getenv("MB_DS_NAME", "yelp_gold")
EXPORT_DIR = pathlib.Path("metabase_export")

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

def get_db_id_by_name(sid, name):
    _, b = api("/api/database", sid)
    lst = json.loads(b)
    dbs = lst.get("data", lst)
    for d in dbs:
        if d.get("name") == name:
            return d["id"]
    raise RuntimeError(f"Datasource '{name}' not found")

def ensure_collection_by_name(sid, name, parent_id=None):
    # list
    _, b = api("/api/collection", sid)
    cols = json.loads(b)
    for c in cols:
        if c.get("name")==name and c.get("archived") is False:
            return c["id"]
    # create
    payload = {"name": name}
    if parent_id: payload["parent_id"] = parent_id
    _, b = api("/api/collection", sid, "POST", payload)
    return json.loads(b)["id"]

def upsert_card(sid, card_obj, db_id, target_collection_id):
    obj = copy.deepcopy(card_obj)
    # normalize fields
    obj.pop("id", None)
    obj["collection_id"] = target_collection_id
    if "dataset_query" in obj and isinstance(obj["dataset_query"], dict):
        dq = obj["dataset_query"]
        if "database" in dq:
            dq["database"] = db_id
    if "database_id" in obj:
        obj["database_id"] = db_id

    # find by name
    _, b = api("/api/card", sid)
    cards = json.loads(b)
    exists = next((x for x in cards if x.get("name")==obj.get("name") and not x.get("archived")), None)
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

    # find by name
    _, b = api("/api/dashboard", sid)
    dashes = json.loads(b)
    exists = next((x for x in dashes if x.get("name")==dash_name and not x.get("archived")), None)
    if exists:
        did = exists["id"]
        # update meta
        meta = {k: obj.get(k) for k in ["name","description","collection_id"]}
        api(f"/api/dashboard/{did}", sid, "PUT", meta)
    else:
        _, rb = api("/api/dashboard", sid, "POST", {"name": dash_name, "collection_id": target_collection_id})
        did = json.loads(rb)["id"]

    # layout (dashboard cards)
    # 读取 export 的 cards 配置（位置、参数）
    ordered_cards = obj.get("ordered_cards") or []
    # 先拉一下现有布局
    _, cur = api(f"/api/dashboard/{did}", sid)
    cur_cards = {c["id"]: c for c in json.loads(cur).get("ordered_cards", [])}

    for oc in ordered_cards:
        # 找到对应 card
        card_ref = oc.get("card") or {}
        card_name = card_ref.get("name")
        if not card_name:  # 可能是文本卡片等
            # 文本/标题卡
            if oc.get("visualization_settings", {}).get("markdown"):
                # 文本卡可以通过 /api/dashboard/{id}/cards 创建，传 text 类型
                payload = {
                    "dashboard_id": did,
                    "parameter_mappings": oc.get("parameter_mappings") or [],
                    "visualization_settings": oc.get("visualization_settings") or {},
                    "col": oc.get("col", 0), "row": oc.get("row", 0),
                    "size_x": oc.get("size_x", 4), "size_y": oc.get("size_y", 3),
                    "cardId": None, "text": oc["visualization_settings"]["markdown"],
                    "series": []
                }
                api(f"/api/dashboard/{did}/cards", sid, "POST", payload)
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
            "visualization_settings": oc.get("visualization_settings") or {},
            "series": oc.get("series") or []
        }
        api(f"/api/dashboard/{did}/cards", sid, "POST", payload)
    return did

def main():
    if not (EXPORT_DIR / "manifest.json").exists():
        print("No export found in metabase_export/. Run mb_export_content.py first.")
        sys.exit(0)

    sid = login()
    db_id = get_db_id_by_name(sid, MB_DS_NAME)

    manifest = json.loads((EXPORT_DIR / "manifest.json").read_text(encoding="utf-8"))
    # 先处理集合（按名字落地；如有嵌套，你可后续扩展 parent_id）
    # 简化：全部落在同名集合下
    exported_cols = manifest.get("collections", [])
    col_name_map = {}  # old_name -> new_id
    for cinfo in exported_cols:
        col_obj = json.loads((EXPORT_DIR / "collections" / f"{cinfo['id']}.json").read_text(encoding="utf-8"))
        new_id = ensure_collection_by_name(sid, col_obj["name"])
        col_name_map[col_obj["name"]] = new_id

    # 卡片：按 collection_id 找到对应集合名（若找不到，落在“Yelp BI”集合）
    default_col_id = ensure_collection_by_name(sid, "Yelp BI")
    name_to_card_id = {}
    for cinfo in manifest.get("cards", []):
        card = json.loads((EXPORT_DIR / "cards" / f"{cinfo['id']}.json").read_text(encoding="utf-8"))
        col_id = default_col_id
        try:
            # 导出对象里通常有 collection_id / collection 名
            col_name = card.get("collection", {}).get("name")
            if col_name and col_name in col_name_map:
                col_id = col_name_map[col_name]
        except Exception:
            pass
        new_cid = upsert_card(sid, card, db_id, col_id)
        name_to_card_id[card.get("name")] = new_cid

    # 仪表盘：落到“Yelp BI”或对应集合
    for dinfo in manifest.get("dashboards", []):
        dash = json.loads((EXPORT_DIR / "dashboards" / f"{dinfo['id']}.json").read_text(encoding="utf-8"))
        col_id = default_col_id
        try:
            col_name = dash.get("collection", {}).get("name")
            if col_name and col_name in col_name_map:
                col_id = col_name_map[col_name]
        except Exception:
            pass
        upsert_dashboard(sid, dash, col_id, name_to_card_id)

    print("✅ Imported/updated collections, cards, dashboards")

if __name__ == "__main__":
    main()