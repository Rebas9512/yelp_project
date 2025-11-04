# pipelines/03_sync_gold_to_pg.py
import argparse, os, duckdb, psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

PG_CONN = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=os.getenv("PG_PORT", "5432"),
    user=os.getenv("PG_USER", "reader"),
    password=os.getenv("PG_PASSWORD", "reader_pw"),
    dbname=os.getenv("PG_DB", "yelp_gold"),
)

def m2d(m): return f"{m}-01"

def open_pg():
    return psycopg2.connect(**PG_CONN)

# ---------------- mart_city_month ----------------
def sync_mart_city_month(month: str) -> int:
    con = duckdb.connect()
    df = con.execute(
        "SELECT * FROM read_parquet('data/gold/mart_city_month.parquet') WHERE review_month = CAST(? AS DATE);",
        [m2d(month)]
    ).df()

    conn = open_pg(); cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS mart_city_month(
        state TEXT, city TEXT, review_month DATE,
        reviews BIGINT, active_businesses BIGINT, active_users BIGINT,
        avg_stars DOUBLE PRECISION,
        PRIMARY KEY(state, city, review_month)
      );
    """)
    cur.execute("DELETE FROM mart_city_month WHERE review_month = %s;", (m2d(month),))
    rows = len(df)
    if rows:
        execute_values(
            cur,
            "INSERT INTO mart_city_month(state,city,review_month,reviews,active_businesses,active_users,avg_stars) VALUES %s",
            df.itertuples(index=False, name=None),
            page_size=10000
        )
    conn.commit(); cur.close(); conn.close()
    print(f"[PG] mart_city_month month={month} synced. rows={rows}")
    return rows

# ---------------- mart_photo_counts ----------------
def sync_mart_photo_counts() -> int:
    """
    优先读取 data/gold/mart_photo_counts.parquet。
    若不存在，则用 dim_photo_files_with_url + silver.business 现算并落盘后再入库。
    """
    con = duckdb.connect()
    mpc_path = Path("data/gold/mart_photo_counts.parquet")

    if not mpc_path.exists():
        con.execute("""
          CREATE OR REPLACE TABLE mart_photo_counts AS
          SELECT
            b.state,
            b.city,
            COALESCE(f.label, '(unknown)') AS label,
            COUNT(*)::BIGINT AS photos
          FROM read_parquet('data/gold/dim_photo_files_with_url.parquet') f
          JOIN read_parquet('data/silver/business/part.parquet') b USING (business_id)
          GROUP BY 1,2,3
          ORDER BY 1,2,3;
        """)
        con.execute("""
          COPY mart_photo_counts TO 'data/gold/mart_photo_counts.parquet'
          (FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE TRUE);
        """)
        print("[gold] materialized data/gold/mart_photo_counts.parquet")

    df = con.execute(
        "SELECT state, city, label, photos FROM read_parquet('data/gold/mart_photo_counts.parquet');"
    ).df()

    conn = open_pg(); cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS mart_photo_counts(
        state TEXT,
        city TEXT,
        label TEXT,
        photos BIGINT,
        PRIMARY KEY(state, city, label)
      );
    """)
    cur.execute("TRUNCATE mart_photo_counts;")

    rows = len(df)
    if rows:
        execute_values(
            cur,
            "INSERT INTO mart_photo_counts(state,city,label,photos) VALUES %s",
            df.itertuples(index=False, name=None),
            page_size=10000
        )
    conn.commit(); cur.close(); conn.close()
    print(f"[PG] mart_photo_counts synced. rows={rows}")
    return rows

# ---------------- dim_user（新增） ----------------
def sync_dim_user(batch_size: int = 50000) -> int:
    """
    一次性全量同步 dim_user（静态数据）。
    策略：TRUNCATE 后全量插入；建立常用索引（year、review_count、average_stars）。
    依赖 data/gold/dim_user.parquet 已由 silver→gold 流程生成。
    """
    con = duckdb.connect()
    df = con.execute("""
        SELECT
          user_id::TEXT,
          name::TEXT,
          review_count::BIGINT,
          CAST(yelping_since AS DATE) AS yelping_since,
          CAST(yelping_year AS INTEGER) AS yelping_year,
          friends_count::BIGINT,
          useful::BIGINT,
          funny::BIGINT,
          cool::BIGINT,
          fans::BIGINT,
          CAST(average_stars AS DOUBLE) AS average_stars
        FROM read_parquet('data/gold/dim_user.parquet')
    """).df()

    conn = open_pg(); cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS dim_user(
        user_id TEXT PRIMARY KEY,
        name TEXT,
        review_count BIGINT,
        yelping_since DATE,
        yelping_year INTEGER,
        friends_count BIGINT,
        useful BIGINT,
        funny BIGINT,
        cool BIGINT,
        fans BIGINT,
        average_stars DOUBLE PRECISION
      );
    """)
    cur.execute("TRUNCATE dim_user;")

    rows = len(df)
    if rows:
        # 分批 execute_values，避免一次性 SQL 过大
        buf = []
        for tup in df.itertuples(index=False, name=None):
            buf.append(tup)
            if len(buf) >= batch_size:
                execute_values(
                    cur,
                    """INSERT INTO dim_user
                       (user_id,name,review_count,yelping_since,yelping_year,
                        friends_count,useful,funny,cool,fans,average_stars)
                       VALUES %s""",
                    buf, page_size=10000
                )
                buf.clear()
        if buf:
            execute_values(
                cur,
                """INSERT INTO dim_user
                   (user_id,name,review_count,yelping_since,yelping_year,
                    friends_count,useful,funny,cool,fans,average_stars)
                   VALUES %s""",
                buf, page_size=10000
            )

    # 索引（幂等）
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_user_yyear ON dim_user(yelping_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_user_reviews ON dim_user(review_count);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_user_avgstars ON dim_user(average_stars);")

    conn.commit(); cur.close(); conn.close()
    print(f"[PG] dim_user synced. rows={rows}")
    return rows

# ---------------- CLI ----------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", help="YYYY-MM（可选：仅同步该月的 mart_city_month；缺省则跳过这个表）")
    ap.add_argument("--all", action="store_true", help="同步所有表（mart_photo_counts + dim_user；如配合 --month 则加上 mart_city_month）")
    ap.add_argument("--only-user", action="store_true", help="仅同步 dim_user")
    ap.add_argument("--only-photo", action="store_true", help="仅同步 mart_photo_counts")
    ap.add_argument("--only-city-month", action="store_true", help="仅同步 mart_city_month（需 --month）")
    args = ap.parse_args()

    any_done = False

    if args.only_user:
        sync_dim_user(); any_done = True
    if args.only_photo:
        sync_mart_photo_counts(); any_done = True
    if args.only_city_month:
        if not args.month:
            raise SystemExit("--only-city-month 需要配合 --month=YYYY-MM")
        sync_mart_city_month(args.month); any_done = True

    if args.all or not any_done:
        # 默认：全量 photo + user；若加 --month 再同步城月
        sync_mart_photo_counts()
        sync_dim_user()
        if args.month:
            sync_mart_city_month(args.month)