#!/usr/bin/env bash
# 端到端演示脚本：对本地运行的网页（默认 127.0.0.1:8000）演示
# 两阶段换账、导入期间用现用账核查/补登、换账冲突退回。
set -euo pipefail
BASE="${1:-http://127.0.0.1:8000}"
DIR="$(cd "$(dirname "$0")" && pwd)"
PY="python3"

post_anchors() {
  local file="$1"
  while IFS=, read -r ev utc; do
    [ "$ev" = "事件号" ] && continue
    curl -s -X POST "$BASE/api/anchors" -H 'Content-Type: application/json' \
      -d "{\"event_no\":$ev,\"utc_ms\":$utc}" >/dev/null
  done < "$file"
}

jqget() { "$PY" -c "import json,sys; print(json.load(sys.stdin)$1)"; }

echo "== 1. 初始建账（events.csv，6 事件）并登记正确锚点 =="
curl -s -F "file=@$DIR/events.csv" "$BASE/api/dataset" | "$PY" -m json.tool | head -10
post_anchors "$DIR/anchors_good.csv"
echo "-- 现用账核查：期望 consistent --"
curl -s -X POST "$BASE/api/verify" | "$PY" -m json.tool

echo; echo "== 2. 两阶段换账：先校验并暂存新账（big_events.csv，不影响现用账） =="
STAGE=$(curl -s -F "file=@$DIR/big_events.csv" "$BASE/api/imports/start")
echo "$STAGE" | "$PY" -m json.tool
IMP_ID=$(echo "$STAGE" | jqget '["import_meta"]["id"]')

echo; echo "-- 2a. 暂存期间继续用现用账核查（dataset_id 仍为 1，结论 consistent） --"
curl -s -X POST "$BASE/api/verify" | "$PY" -m json.tool

echo; echo "-- 2b. 暂存期间向现用账补登锚点（事件 2） --"
curl -s -X POST "$BASE/api/anchors" -H 'Content-Type: application/json' \
  -d '{"event_no":2,"utc_ms":1700000001000}' | "$PY" -m json.tool

echo; echo "== 3. 坏行文件：整份校验失败，连暂存都不会生成，现用账与锚点保留 =="
curl -s -F "file=@$DIR/events_bad.csv" "$BASE/api/imports/start" | "$PY" -m json.tool
echo "-- 当前现用账仍是初始账，锚点仍在 --"
curl -s "$BASE/api/state" | "$PY" -m json.tool | head -30

echo; echo "== 4. 换账冲突：另一份账先生效，暂存账提交被退回（哪怕同名同条数） =="
curl -s -F "file=@$DIR/events2.csv;filename=big_events.csv" "$BASE/api/dataset" >/dev/null
echo "-- 提交暂存账 #$IMP_ID：期望 409 换账冲突 --"
curl -s -o /tmp/conflict.json -w "HTTP %{http_code}\n" \
  -X POST "$BASE/api/imports/$IMP_ID/commit"
cat /tmp/conflict.json | "$PY" -m json.tool
echo "-- 放弃冲突的暂存账 --"
curl -s -X DELETE "$BASE/api/imports/$IMP_ID" | "$PY" -m json.tool

echo; echo "== 5. 正常两阶段换账：再暂存一份并提交，旧锚点随旧账退出核查 =="
IMP2=$(curl -s -F "file=@$DIR/big_events.csv" "$BASE/api/imports/start" | jqget '["import_meta"]["id"]')
curl -s -X POST "$BASE/api/imports/$IMP2/commit" | "$PY" -m json.tool
echo "-- 现用账切换为 big_events.csv，锚点已清空；新账事件号 200 可登记 --"
curl -s -X POST "$BASE/api/anchors" -H 'Content-Type: application/json' \
  -d '{"event_no":200,"utc_ms":1700000000000}' | "$PY" -m json.tool
