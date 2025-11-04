import os, duckdb, pathlib
def get_con():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    return con
def raw_dir() -> str:
    # 允许路径有空格
    return os.getenv("RAW_DIR", "Yelp JSON")
def join_raw(*parts) -> str:
    return str(pathlib.Path(raw_dir(), *parts))
