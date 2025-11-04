import os, duckdb, pandas as pd
def fetch_city_month(state: str, city: str, start: str, end: str) -> pd.DataFrame:
    con = duckdb.connect()
    q = """
    SELECT * FROM mart_city_month
    WHERE state = ? AND city = ?
      AND review_month BETWEEN DATE ? AND DATE ?
    ORDER BY review_month;
    """
    return con.execute(q, [state, city, f"{start}-01", f"{end}-01"]).df()
