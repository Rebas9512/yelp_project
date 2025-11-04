-- 初始化 yelp_gold 数据库、schema 与只读账号 reader
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'yelp_gold') THEN
    EXECUTE 'CREATE DATABASE yelp_gold';
  END IF;
END $$;

\connect yelp_gold

CREATE SCHEMA IF NOT EXISTS yelp_gold;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='reader') THEN
    CREATE ROLE reader LOGIN PASSWORD 'reader_pw';
  END IF;
END $$;

GRANT USAGE ON SCHEMA yelp_gold TO reader;
GRANT SELECT ON ALL TABLES IN SCHEMA yelp_gold TO reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA yelp_gold GRANT SELECT ON TABLES TO reader;
