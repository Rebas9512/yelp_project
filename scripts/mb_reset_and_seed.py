#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reset Metabase app (container) and seed a reproducible configuration:
- Remove and recreate Metabase container (H2 app DB resets with the container).
- Wait for health.
- If uninitialized -> POST /api/setup with MB_EMAIL/MB_PASS.
  Else -> POST /api/session login.
- Delete Sample Database (H2) and any non-target DBs.
- Ensure a single Postgres datasource named `yelp_gold`, scoped to schema `yelp_gold`
  with schema_fallback disabled; trigger sync+rescan.

ENV it uses (from your .env):
  MB_BASE=http://localhost:3000
  MB_EMAIL=admin@yelp.local
  MB_PASS=Metabase!2025
  PG_HOST=localhost
  PG_PORT=5432
  PG_USER=reader
  PG_PASSWORD=reader_pw
  PG_DB=yelp_gold
  PG_SCHEMA=yelp_gold
  (optional) MB_SITE_NAME="Yelp BI"
"""

import json, os, sys, time, subprocess
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# --------- Config from env (robust) ---------
def env(name, default):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

def env_int(name, default):
    v = os.getenv(name)
    try:
        return int(v) if (v is not None and v != "") else int(default)
    except Exception:
        return int(default)

MB_BASE   = env("MB_BASE", "http://localhost:3000").rstrip("/")
MB_EMAIL  = env("MB_EMAIL", "admin@yelp.local")
MB_PASS   = env("MB_PASS",  "Metabase!2025")
MB_SITE   = env("MB_SITE_NAME", "Yelp BI")

PG_HOST   = env("PG_HOST", "localhost")
PG_PORT   = env_int("PG_PORT", 5432)
PG_USER   = env("PG_USER", "reader")
PG_PASSWD = env("PG_PASSWORD", "reader_pw")
PG_DB     = env("PG_DB", "yelp_gold")
PG_SCHEMA = env("PG_SCHEMA", "yelp_gold")

TARGET_DS_NAME = env("MB_DS_NAME", "yelp_gold")
COMPOSE = env("COMPOSE_BIN", "docker compose")
MB_SERVICE = env("MB_SERVICE", "metabase")
PG_SERVICE = env("PG_SERVICE", "postgres")

# From Metabase container's perspective, "localhost" would be itself.
# If user wrote localhost/127.0.0.1 in .env, connect via Compose service name.
if PG_HOST in ("localhost", "127.0.0.1"):
    PG_HOST_FOR_MB = PG_SERVICE  # service name reachable inside the docker network
else:
    PG_HOST_FOR_MB = PG_HOST

# --------- HTTP helpers ----------
def _req(url, method="GET", data=None, headers=None, timeout=15):
    headers = headers or {}
    if data is not None and not isinstance(data, (bytes, bytearray)):
        headers.setdefault("Content-Type", "application/json")
        data = json.dumps(data).encode("utf-8")
    req = Request(url, data=data, headers=headers, method=method)
    with urlopen(req, timeout=timeout) as resp:
        return resp.getcode(), resp.read()

def wait_health(base, secs=180):
    print(f"-> Waiting health: {base}/api/health")
    t0 = time.time()
    while time.time()-t0 < secs:
        try:
            code, body = _req(f"{base}/api/health")
            if code == 200 and (json.loads(body or b"{}").get("status","").lower() == "ok"):
                print("   ✅ Metabase healthy")
                return
        except Exception:
            pass
        print(".", end="", flush=True); time.sleep(2)
    print("")  # newline
    raise RuntimeError("Metabase not healthy in time")

def session_properties(base):
    try:
        _, b = _req(f"{base}/api/session/properties")
        j = json.loads(b or b"{}")
        return {"token": j.get("setup_token") or j.get("setup-token"),
                "is_setup": j.get("is_setup")}
    except Exception:
        return {"token": None, "is_setup": None}

def login(base, email, password):
    c, b = _req(f"{base}/api/session","POST",
                {"username": email, "password": password})
    j = json.loads(b or b"{}")
    if c==200 and j.get("id"): return j["id"]
    raise HTTPError(f"{base}/api/session", c, "login failed", hdrs=None, fp=None)

def do_setup(base, email, password, site_name, token):
    variants = [
        {"token": token, "user": {"first_name":"Admin","last_name":"User","email":email,"password":password},
         "site_name": site_name, "prefs":{"allow_tracking": False}},
        {"token": token, "user": {"first_name":"Admin","last_name":"User","email":email,"password":password},
         "prefs":{"site_name": site_name, "allow_tracking": False}},
    ]
    last = None
    for p in variants:
        try:
            c, b = _req(f"{base}/api/setup","POST",p)
            j = json.loads(b or b"{}")
            if c==200 and j.get("id"): return j["id"]
            last = RuntimeError(f"setup unexpected {c} {b!r}")
        except Exception as e:
            last = e
    raise RuntimeError(f"/api/setup failed: {last}")

def api(base, path, sid, method="GET", data=None):
    return _req(f"{base}{path}", method, data, {"X-Metabase-Session": sid})

# --------- Compose helpers ----------
def sh(cmd: list, check=True):
    print("$"," ".join(cmd))
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0 and check:
        print(r.stdout)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return r.stdout

def recreate_metabase():
    # Remove Metabase container so H2 app DB is reset (no volume is mounted).
    # Do not touch postgres/minio volumes.
    try:
        sh(COMPOSE.split() + ["stop", MB_SERVICE], check=False)
        sh(COMPOSE.split() + ["rm", "-f", MB_SERVICE], check=False)
    except Exception:
        pass
    # Ensure Postgres is up, then bring Metabase up
    sh(COMPOSE.split() + ["up", "-d", PG_SERVICE])
    sh(COMPOSE.split() + ["up", "-d", MB_SERVICE])

# --------- Datasource ensure ----------
def list_databases(base, sid):
    _, b = api(base, "/api/database", sid)
    lst = json.loads(b or b"[]")
    return lst.get("data", lst)  # old/new API forms

def delete_database(base, sid, db_id):
    api(base, f"/api/database/{db_id}", sid, "DELETE", None)

def ensure_target_pg(base, sid):
    # prepare details with only schema `yelp_gold` exposed
    details = {
        "host": PG_HOST_FOR_MB,
        "port": PG_PORT,
        "dbname": PG_DB,
        "user": PG_USER,
        "password": PG_PASSWD,
        "ssl": False,
        "schemas": [PG_SCHEMA],
        "schema_fallback": False
    }
    payload = {
        "name": TARGET_DS_NAME,
        "engine": "postgres",
        "details": details,
        "is_full_sync": True
    }
    dbs = list_databases(base, sid)
    found = next((d for d in dbs if d.get("name")==TARGET_DS_NAME), None)

    if not found:
        print(f"-> Creating datasource '{TARGET_DS_NAME}' ...")
        c, b = api(base, "/api/database", sid, "POST", payload)
        if c not in (200,201):
            raise RuntimeError(f"create datasource failed: {c} {b!r}")
        found = json.loads(b or b"{}")
    else:
        print(f"-> Updating datasource '{TARGET_DS_NAME}' ...")
        db_id = found["id"]
        _, cur_b = api(base, f"/api/database/{db_id}", sid)
        cur = json.loads(cur_b or b"{}")
        cur["details"] = details
        c, b = api(base, f"/api/database/{db_id}", sid, "PUT", cur)
        if c != 200:
            raise RuntimeError(f"update datasource failed: {c} {b!r}")
        found = json.loads(b or b"{}")

    db_id = found["id"]
    print("-> Sync schema & rescan values ...")
    api(base, f"/api/database/{db_id}/sync_schema", sid, "POST", {})
    api(base, f"/api/database/{db_id}/rescan_values", sid, "POST", {})
    print(f"   ✅ datasource ready (id={db_id})")

    # Now delete extra databases (Sample DB etc.)
    print("-> Removing extra databases (Sample DB, others) ...")
    dbs = list_databases(base, sid)
    for d in dbs:
        if d.get("id") == db_id:
            continue
        name = (d.get("name") or "").lower()
        engine = (d.get("engine") or "").lower()
        # Remove known sample or any non-target DBs
        if "sample" in name or engine in ("h2", "bigquery", "mysql", "sqlite", "mongo", "snowflake", "redshift", "druid", "presto", "trino") or name != TARGET_DS_NAME:
            try:
                print(f"   - delete DB id={d['id']} name={d.get('name')} engine={d.get('engine')}")
                delete_database(base, sid, d["id"])
            except Exception as e:
                print(f"     ! delete failed (ignored): {e}")

def main():
    print("== RESET & SEED METABASE ==")
    recreate_metabase()
    wait_health(MB_BASE)

    # setup or login
    sid = None
    try:
        print(f"-> Login as {MB_EMAIL}")
        sid = login(MB_BASE, MB_EMAIL, MB_PASS)
        print("   ✅ login ok")
    except HTTPError as e:
        if e.code == 401:
            token = session_properties(MB_BASE).get("token")
            if token:
                print("-> Uninitialized instance. Running setup ...")
                sid = do_setup(MB_BASE, MB_EMAIL, MB_PASS, MB_SITE, token)
                print("   ✅ setup ok")
            else:
                print("❌ Unauthorized and no setup_token; wrong MB_EMAIL/MB_PASS?", file=sys.stderr)
                sys.exit(1)
        else:
            raise

    ensure_target_pg(MB_BASE, sid)
    print("\nAll done. Metabase is reproducibly configured ✅")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Failed:", e, file=sys.stderr)
        sys.exit(1)