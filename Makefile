# =================== Minimal Metabase + Export Makefile ===================
SHELL := /bin/bash

# ---- docker compose exec helper (jupyter 容器里跑命令) ----
JUP_RUN = docker compose exec -T jupyter bash -lc

# ---- Metabase 基本配置（可被 .env 覆盖）----
MB_BASE  ?= http://localhost:3000
MB_EMAIL ?= admin@yelp.local
MB_PASS  ?= Metabase!2025

# ---- Postgres schema 限定（用于刷新时限制到 yelp_gold）----
PG_SCHEMA ?= yelp_gold

# ---- Compose 服务名（service 名，不是 container_name）----
PG_SERVICE        ?= postgres
JUPYTER_SERVICE   ?= jupyter
METABASE_SERVICE  ?= metabase
NGINX_SERVICE     ?= nginx

# ---- 导出脚本位置 ----
EXPORT_SCRIPT := scripts/export_pg_yelp_gold.py
.PHONY: clean down export export-csv export-sql help logs mb-health mb-open mb-refresh ps restart ui up

# 一步登录并打开 Metabase（在宿主机运行，保证本机浏览器能打开）
ui mb-open:
	@chmod +x scripts/mb_one_click_login.py
	@MB_BASE="$(MB_BASE)" MB_EMAIL="$(MB_EMAIL)" MB_PASS="$(MB_PASS)" \
		python3 scripts/mb_one_click_login.py

# 刷新 Metabase：仅保留 $(PG_SCHEMA) schema，关闭 schema_fallback，并触发元数据同步与字段重扫
mb-refresh:
	@set -euo pipefail; \
	command -v jq >/dev/null 2>&1 || { echo "❌ 需要安装 jq"; exit 1; }; \
	echo "-> Logging in to $(MB_BASE) ..."; \
	SID=$$(curl -sS -X POST "$(MB_BASE)/api/session" \
	  -H 'Content-Type: application/json' \
	  -d "$$(printf '{"username":"%s","password":"%s"}' '$(MB_EMAIL)' '$(MB_PASS)')" \
	  | jq -r .id); \
	test -n "$$SID" -a "$$SID" != "null" || { echo "❌ 登录失败"; exit 1; }; \
	echo "   session: $$SID"; \
	DBS_JSON=$$(curl -sS -H "X-Metabase-Session: $$SID" "$(MB_BASE)/api/database"); \
	DB_ID=$$(echo "$$DBS_JSON" | jq -r '(.data // .) | map(select(.name | test("yelp_gold"; "i"))) | first | .id'); \
	test -n "$$DB_ID" -a "$$DB_ID" != "null" || { echo "❌ 未找到 yelp_gold 数据源"; exit 2; }; \
	echo "   database id: $$DB_ID"; \
	curl -sS -H "X-Metabase-Session: $$SID" "$(MB_BASE)/api/database/$$DB_ID" \
	  | jq '.details.schemas=["$(PG_SCHEMA)"] | .details.schema_fallback=false' \
	  > /tmp/mb_db_$${DB_ID}_update.json; \
	curl -sS -X PUT -H "X-Metabase-Session: $$SID" -H 'Content-Type: application/json' \
	  --data-binary @/tmp/mb_db_$${DB_ID}_update.json \
	  "$(MB_BASE)/api/database/$$DB_ID" >/dev/null; \
	curl -sS -X POST -H "X-Metabase-Session: $$SID" "$(MB_BASE)/api/database/$$DB_ID/sync_schema"   >/dev/null; \
	curl -sS -X POST -H "X-Metabase-Session: $$SID" "$(MB_BASE)/api/database/$$DB_ID/rescan_values" >/dev/null; \
	echo "✅ Metabase refreshed (db=$$DB_ID, schema=$(PG_SCHEMA))"

# 最小健康检查
mb-health:
	@set -euo pipefail; \
	echo "==> Check Metabase health"; \
	curl -sS "$(MB_BASE)/api/health" | jq -r .status | grep -q '^ok$$' \
	  && echo "Metabase OK" || (echo "Metabase not healthy"; exit 1)

# ------------------- 一步式导出（CSV + SQL + 可选刷新MB） -------------------
export:
	@echo "==> Ensure containers are up"
	@docker compose up -d $(PG_SERVICE) $(JUPYTER_SERVICE) minio >/dev/null
	@echo "==> Refresh Metabase then export CSV + SQL"
	@$(JUP_RUN) ' \
	  MB_BASE_RAW="$(MB_BASE)"; \
	  case "$$MB_BASE_RAW" in \
	    http://localhost:3000|http://127.0.0.1:3000|https://localhost:3000|https://127.0.0.1:3000) MB_BASE_CTN="http://metabase:3000" ;; \
	    *) MB_BASE_CTN="$$MB_BASE_RAW" ;; \
	  esac; \
	  MB_BASE="$$MB_BASE_CTN" MB_EMAIL="$(MB_EMAIL)" MB_PASS="$(MB_PASS)" \
	  PG_HOST="yelp_pg" PG_PORT="5432" PG_USER="reader" PG_PASSWORD="reader_pw" PG_DB="yelp_gold" PG_SCHEMA="$(PG_SCHEMA)" \
	  python $(EXPORT_SCRIPT) --metabase-refresh \
	'

export-csv:
	@docker compose up -d $(PG_SERVICE) $(JUPYTER_SERVICE) >/dev/null
	@echo "==> Export CSV only"
	@$(JUP_RUN) ' \
	  PG_HOST="yelp_pg" PG_PORT="5432" PG_USER="reader" PG_PASSWORD="reader_pw" PG_DB="yelp_gold" PG_SCHEMA="$(PG_SCHEMA)" \
	  python $(EXPORT_SCRIPT) --csv \
	'

export-sql:
	@docker compose up -d $(PG_SERVICE) $(JUPYTER_SERVICE) >/dev/null
	@echo "==> Export SQL only"
	@$(JUP_RUN) ' \
	  PG_HOST="yelp_pg" PG_PORT="5432" PG_USER="reader" PG_PASSWORD="reader_pw" PG_DB="yelp_gold" PG_SCHEMA="$(PG_SCHEMA)" \
	  python $(EXPORT_SCRIPT) --sql \
	'
# ========================================================================

# ------------------- Convenience targets -------------------
## up: 启动核心服务（Postgres/Metabase/Jupyter/Nginx）
up:
	@docker compose up -d $(PG_SERVICE) $(METABASE_SERVICE) $(JUPYTER_SERVICE) $(NGINX_SERVICE)
	@docker compose ps

## down: 停止并移除容器（不删数据卷）
down:
	@docker compose down

## restart: 重启核心服务
restart: down up

## ps: 查看容器状态
ps:
	@docker compose ps

## logs: 查看所有服务日志（最新 200 行）
logs:
	@docker compose logs --no-color --tail=200

## logs-%: 查看指定服务日志（如：make logs-metabase）
logs-%:
	@docker compose logs --no-color --tail=200 $*

## clean: 清理缓存/临时文件
clean:
	@find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "✅ cleaned."

## help: 显示常用命令帮助
help:
	@echo "Available targets:"
	@grep -E '^[a-zA-Z0-9_.%-]+:|^## ' Makefile | \
		awk 'BEGIN{FS=":|##"} /^[a-zA-Z0-9_.%-]+:/{t=$$1} /^##/{gsub(/^[ \t]+|[ \t]+$$/,"",$$2); if(t!="") {printf "  \033[36m%-18s\033[0m %s\n", t, $$2; t=""}}'
# -----------------------------------------------------------
