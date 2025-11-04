#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced exporter for schema 'yelp_gold'

Features
- (Optional) Metabase refresh before export (limit to yelp_gold schema)
- CSV for all BASE TABLEs under target schema (optionally gzip)
- SQL dumps: schema-only, data-only, full (smart fallback to docker pg_dump)
- Per-object DDL split (tables/views/materialized views) [can be disabled]
- Include/Exclude table filters
- Custom output dir; manifest + restore helper

Env (defaults align with docker-compose):
  PG_HOST=yelp_pg     # 宿主机上建议覆盖为 localhost
  PG_PORT=5432
  PG_USER=reader
  PG_PASSWORD=reader_pw
  PG_DB=yelp_gold
  PG_SCHEMA=yelp_gold

Metabase (optional, for --metabase-refresh):
  MB_BASE=http://localhost:3000
  MB_EMAIL=admin@yelp.local
  MB_PASS=Metabase!2025
  MB_DB_ID=2                     # 如果未知，脚本会按名称匹配含 "yelp_gold" 的数据库对象
  MB_DB_NAME_HINT=yelp_gold      # 用于按名称匹配

Docker fallback:
  PG_DOCKER=1                    # 优先用 docker compose exec -T yelp_pg 调 pg_dump
  PG_CONTAINER=yelp_pg           # 容器名
"""

import os
import re
import sys
import csv
import json
import time
import shlex
import pathlib
import argparse
import subprocess
from typing import List, Tuple, Optional

import psycopg2
from psycopg2 import sql as psql

# ------------ CLI args ------------
ap = argparse.ArgumentParser(description="Export yelp_gold (CSV + SQL) with optional Metabase refresh.")
ap.add_argument("--metabase-refresh", action="store_true",
                help="Before export: limit schema to yelp_gold and run sync_schema + rescan_values")
ap.add_argument("--no-ddl", action="store_true", help="Skip per-object DDL files under sql_*/ddl")
ap.add_argument("--force-docker-pgdump", action="store_true",
                help="Always use docker-compose exec to run pg_dump inside PG container")
ap.add_argument("--csv", action="store_true", help="Only export CSV (skip SQL)")
ap.add_argument("--sql", action="store_true", help="Only export SQL (skip CSV)")
ap.add_argument("--gzip", action="store_true", help="Compress CSV files to .csv.gz")
ap.add_argument("--outdir", type=str, default="exports", help="Output base directory (default: exports)")
ap.add_argument("--schema", type=str, default=None, help="Override target schema (default: env PG_SCHEMA)")
ap.add_argument("--include", type=str, nargs="*", default=[],
                help="Only export these tables (names or schema.table). If set, others are skipped.")
ap.add_argument("--exclude", type=str, nargs="*", default=[],
                help="Exclude these tables (names or schema.table).")
args = ap.parse_args()

# ------------ Auto environment detection ------------
def inside_container() -> bool:
    return pathlib.Path("/.dockerenv").exists()

def default_pg_host() -> str:
    # 容器内默认用 docker 网络名；宿主机默认用 localhost
    if os.getenv("PG_HOST"):
        return os.getenv("PG_HOST")
    return "yelp_pg" if inside_container() else "localhost"

# ------------ PG config ------------
PG_HOST   = default_pg_host()
PG_PORT   = int(os.getenv("PG_PORT", "5432"))
PG_USER   = os.getenv("PG_USER", "reader")
PG_PW     = os.getenv("PG_PASSWORD", "reader_pw")
PG_DB     = os.getenv("PG_DB", "yelp_gold")
PG_SCHEMA = args.schema or os.getenv("PG_SCHEMA", "yelp_gold")

# ------------ Metabase config ------------
MB_BASE = os.getenv("MB_BASE", os.getenv("MB_SITE_URL", "http://localhost:3000"))
MB_EMAIL = os.getenv("MB_EMAIL", "admin@yelp.local")
MB_PASS  = os.getenv("MB_PASS",  "Metabase!2025")
MB_DB_ID = os.getenv("MB_DB_ID")  # optional
MB_DB_NAME_HINT = os.getenv("MB_DB_NAME_HINT", "yelp_gold")

# ------------ Docker fallback ------------
PG_DOCKER = os.getenv("PG_DOCKER", "0") == "1"
PG_CONTAINER = os.getenv("PG_CONTAINER", "yelp_pg")

# ------------ Paths ------------
ROOT = pathlib.Path(__file__).resolve().parents[1]
EXPORT_DIR = (ROOT / args.outdir).resolve()
ts = time.strftime("%Y%m%d_%H%M%S")
CSV_DIR = EXPORT_DIR / f"csv_{ts}"
SQL_DIR = EXPORT_DIR / f"sql_{ts}"
DDL_DIR = SQL_DIR / "ddl"

CSV_DIR.mkdir(parents=True, exist_ok=True)
SQL_DIR.mkdir(parents=True, exist_ok=True)
DDL_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    print(msg, flush=True)

# ------------ PG helpers ------------
def pg_connect():
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PW
        )
        with conn.cursor() as cur:
            cur.execute("SET client_encoding TO 'UTF8';")
        return conn
    except Exception as e:
        tip = ""
        if "could not translate host name" in str(e) and PG_HOST == "yelp_pg" and not inside_container():
            tip = "（当前在宿主机运行，建议设置 PG_HOST=localhost）"
        if "does not exist" in str(e) and "role" in str(e):
            tip = "（确认你的数据库里确实存在用户/角色 reader）"
        raise RuntimeError(f"Postgres 连接失败: {e} {tip}")

def sanitize_filename(name: str) -> str:
    return re.sub(r"[^0-9A-Za-z._-]", "_", name)

def _qualify(name: str) -> Tuple[str, str]:
    if "." in name:
        s, t = name.split(".", 1)
        return s.strip('"'), t.strip('"')
    return PG_SCHEMA, name

def list_base_tables(conn) -> List[Tuple[str,str]]:
    sql = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema=%s AND table_type='BASE TABLE'
    ORDER BY 1,2;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (PG_SCHEMA,))
        return cur.fetchall()

def list_views(conn) -> List[Tuple[str,str]]:
    sql = """
    SELECT table_schema, table_name
    FROM information_schema.views
    WHERE table_schema=%s
    ORDER BY 1,2;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (PG_SCHEMA,))
        return cur.fetchall()

def list_matviews(conn) -> List[Tuple[str,str]]:
    sql = """
    SELECT schemaname, matviewname
    FROM pg_matviews
    WHERE schemaname=%s
    ORDER BY 1,2;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (PG_SCHEMA,))
        return cur.fetchall()

def copy_table_to_csv(conn, schema: str, table: str, out_path: pathlib.Path, gzip: bool):
    fq = psql.SQL("{}.{}").format(psql.Identifier(schema), psql.Identifier(table)).as_string(conn)
    if gzip:
        import gzip
        with conn.cursor() as cur, gzip.open(out_path.with_suffix(out_path.suffix + ".gz"), "wt", newline="", encoding="utf-8") as f:
            cur.copy_expert(f'COPY (SELECT * FROM {fq}) TO STDOUT WITH CSV HEADER;', f)
    else:
        with conn.cursor() as cur, out_path.open("w", newline="", encoding="utf-8") as f:
            cur.copy_expert(f'COPY (SELECT * FROM {fq}) TO STDOUT WITH CSV HEADER;', f)

# ------------ pg_dump runners ------------
def _have(cmd: str) -> bool:
    return subprocess.call(["bash","-lc",f"command -v {shlex.quote(cmd)} >/dev/null 2>&1"]) == 0

def run_local_pgdump(args: List[str], out_path: pathlib.Path) -> None:
    cmd = ["pg_dump", "-h", PG_HOST, "-p", str(PG_PORT), "-U", PG_USER, PG_DB] + args
    env = os.environ.copy()
    env["PGPASSWORD"] = PG_PW
    log(f"+ {' '.join(shlex.quote(x) if x != PG_PW else '****' for x in cmd)}")
    with out_path.open("wb") as f:
        subprocess.run(cmd, check=True, env=env, stdout=f)

def run_docker_pgdump(args: List[str], out_path: pathlib.Path) -> None:
    if not _have("docker"):
        raise RuntimeError("未检测到 docker 命令，无法 docker 方式导出。")
    inner = ["PGPASSWORD="+PG_PW, "pg_dump", "-h", "localhost", "-p", "5432", "-U", PG_USER, PG_DB] + args
    cmd = ["docker", "compose", "exec", "-T", PG_CONTAINER, "bash", "-lc", shlex.quote(" ".join(inner))]
    log(f"+ docker compose exec -T {PG_CONTAINER} pg_dump {' '.join(shlex.quote(a) for a in args)}")
    with out_path.open("wb") as f:
        subprocess.run(cmd, check=True, stdout=f)

class DumpArgs:
    def __init__(self, args: List[str], force_docker: bool = False):
        self.args = args
        self.force_docker = force_docker

def run_pgdump_smart(dargs: DumpArgs, out_path: pathlib.Path) -> None:
    if dargs.force_docker or PG_DOCKER:
        return run_docker_pgdump(dargs.args, out_path)
    try:
        return run_local_pgdump(dargs.args, out_path)
    except subprocess.CalledProcessError as e:
        # 常见原因：client/server 版本不匹配，回退 docker 内执行
        log(f"local pg_dump failed (code {e.returncode}), trying docker fallback …")
        return run_docker_pgdump(dargs.args, out_path)

def dump_schema_all(force_docker: bool = False):
    run_pgdump_smart(DumpArgs(["-n", PG_SCHEMA, "-s"], force_docker), SQL_DIR / "schema_only.sql")
    run_pgdump_smart(DumpArgs(["-n", PG_SCHEMA, "-a"], force_docker), SQL_DIR / "data_only.sql")
    run_pgdump_smart(DumpArgs(["-n", PG_SCHEMA], force_docker), SQL_DIR / "full_dump.sql")

def dump_per_object_ddl(conn, force_docker: bool = False):
    for sch, tbl in list_base_tables(conn):
        out = DDL_DIR / f"{sanitize_filename(sch)}.{sanitize_filename(tbl)}.table.sql"
        run_pgdump_smart(DumpArgs(["-s", "-t", f"{sch}.{tbl}"], force_docker), out)
    for sch, v in list_views(conn):
        out = DDL_DIR / f"{sanitize_filename(sch)}.{sanitize_filename(v)}.view.sql"
        run_pgdump_smart(DumpArgs(["-s", "-t", f"{sch}.{v}"], force_docker), out)
    for sch, mv in list_matviews(conn):
        out = DDL_DIR / f"{sanitize_filename(sch)}.{sanitize_filename(mv)}.mview.sql"
        run_pgdump_smart(DumpArgs(["-s", "-t", f"{sch}.{mv}"], force_docker), out)

# ------------ Metabase helpers ------------
def mb_req(path: str, method: str = "GET", data: Optional[dict] = None, headers: Optional[dict] = None) -> bytes:
    import urllib.request
    headers = headers or {}
    url = MB_BASE.rstrip("/") + path
    if data is not None and not isinstance(data, (bytes, bytearray)):
        data = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()

def metabase_refresh():
    sid = json.loads(mb_req("/api/session", "POST", {"username": MB_EMAIL, "password": MB_PASS}))["id"]
    hdr = {"X-Metabase-Session": sid}
    db_id = MB_DB_ID
    if not db_id:
        dbs = json.loads(mb_req("/api/database", "GET", None, hdr))
        cand = (dbs.get("data") if isinstance(dbs, dict) else dbs) or []
        for d in cand:
            if MB_DB_NAME_HINT.lower() in d.get("name", "").lower():
                db_id = d["id"]; break
    if not db_id:
        log("Metabase: could not resolve DB id; skipping schema narrowing.")
    else:
        db_obj = json.loads(mb_req(f"/api/database/{db_id}", "GET", None, hdr))
        db_obj["details"]["schemas"] = [PG_SCHEMA]
        db_obj["details"]["schema_fallback"] = False
        mb_req(f"/api/database/{db_id}", "PUT", db_obj, hdr)
        mb_req(f"/api/database/{db_id}/sync_schema", "POST", None, hdr)
        mb_req(f"/api/database/{db_id}/rescan_values", "POST", None, hdr)
        log(f"Metabase refreshed for DB id={db_id}")

# ------------ include/exclude filters ------------
def _name_matches(name_schema: str, name_table: str, patterns: List[str]) -> bool:
    """patterns 可以是 'table' 或 'schema.table'，大小写不敏感"""
    full = f"{name_schema}.{name_table}".lower()
    for p in patterns:
        p = p.lower()
        if "." in p:
            if full == p:
                return True
        else:
            if name_table.lower() == p:
                return True
    return False

# ------------ main ------------
def main():
    # optional metabase refresh
    if args.metabase_refresh:
        log("==> Metabase refresh (limit schema + sync + rescan)")
        try:
            metabase_refresh()
        except Exception as e:
            log(f"⚠️  Metabase refresh failed (ignored): {e}")

    only_csv = args.csv and not args.sql
    only_sql = args.sql and not args.csv

    log(f"Export target schema: {PG_SCHEMA}")
    log(f"CSV  -> {CSV_DIR}")
    log(f"SQL  -> {SQL_DIR}")

    conn = pg_connect()
    try:
        # Resolve export tables (with include/exclude)
        tables = list_base_tables(conn)
        if args.include:
            tables = [t for t in tables if _name_matches(t[0], t[1], args.include)]
        if args.exclude:
            tables = [t for t in tables if not _name_matches(t[0], t[1], args.exclude)]

        # CSV
        if not only_sql:
            if not tables:
                log(f"⚠️  No base tables to export in schema {PG_SCHEMA}")
            else:
                total = 0
                for sch, tbl in tables:
                    base = f"{sanitize_filename(sch)}.{sanitize_filename(tbl)}.csv"
                    out = CSV_DIR / base
                    log(f"[CSV] {sch}.{tbl} -> {out.name}{'.gz' if args.gzip else ''}")
                    copy_table_to_csv(conn, sch, tbl, out, gzip=args.gzip)
                    total += 1
                log(f"✅ CSV export done. tables={total}")

        # SQL
        if not only_csv:
            force_docker = bool(args.force_docker_pgdump)
            log("[SQL] dumping schema/data …")
            dump_schema_all(force_docker=force_docker)

            if not args.no_ddl:
                log("[SQL] dumping per-object DDL …")
                dump_per_object_ddl(conn, force_docker=force_docker)

            # manifest + restore helper
            manifest = SQL_DIR / "MANIFEST.txt"
            helper   = SQL_DIR / "RESTORE_HELP.txt"
            with manifest.open("w", encoding="utf-8") as f:
                f.write(f"yelp_gold export @ {ts}\n")
                f.write(f"Host={PG_HOST} Port={PG_PORT} DB={PG_DB} Schema={PG_SCHEMA}\n")
                f.write("Files:\n")
                for p in sorted(SQL_DIR.glob("*.sql")):
                    f.write(f"  - {p.name}\n")
                if not args.no_ddl:
                    f.write("Per-object DDL under ddl/\n")
            with helper.open("w", encoding="utf-8") as f:
                f.write("# Restore examples\n")
                f.write("# 1) schema only\n")
                f.write(f"psql -h <host> -p <port> -U <user> -d {PG_DB} -f schema_only.sql\n\n")
                f.write("# 2) data only\n")
                f.write(f"psql -h <host> -p <port> -U <user> -d {PG_DB} -f data_only.sql\n\n")
                f.write("# 3) full dump\n")
                f.write(f"psql -h <host> -p <port> -U <user> -d {PG_DB} -f full_dump.sql\n")

            log(f"📄 Manifest: {manifest}")
            log(f"📄 Restore helper: {helper}")

        log("✅ Export complete.")
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"❌ pg_dump failed (code {e.returncode})", file=sys.stderr)
        sys.exit(e.returncode)
    except Exception as e:
        print(f"❌ Failed: {e}", file=sys.stderr)
        sys.exit(1)