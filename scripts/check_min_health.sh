#!/usr/bin/env bash
set -euo pipefail

MB_BASE="${MB_BASE:-http://localhost:3000}"
echo "-> Checking Metabase at ${MB_BASE} ..."
curl -sS "${MB_BASE}/api/health" | grep -q '"status":"ok"' && echo "✅ Metabase OK" || { echo "❌ Metabase not healthy"; exit 1; }

echo "-> Checking Postgres container readiness ..."
docker compose exec -T postgres pg_isready -U reader -d yelp_gold
echo "✅ Postgres OK"
