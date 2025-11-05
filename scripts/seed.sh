#!/usr/bin/env bash
set -euo pipefail

# Seed script for Trino + Iceberg (Nessie)
# - Waits for Trino to finish starting
# - Creates Iceberg schema/table and inserts sample rows

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TRINO_SERVER="http://localhost:8080"
TRINO_EXEC_PREFIX=(docker exec trino trino --server "$TRINO_SERVER" --output-format CSV)

if ! docker ps --format '{{.Names}}' | grep -q '^trino$'; then
  echo "[seed] ERROR: 'trino' container not running. Start the stack first (docker compose up -d)." >&2
  exit 1
fi

echo "[seed] Waiting for Trino to accept queries (timeout ~30s)..."
for i in $(seq 1 30); do
  if timeout 1s "${TRINO_EXEC_PREFIX[@]}" --execute "SELECT 1" >/dev/null 2>&1; then
    echo "[seed] Trino is ready."
    break
  fi
  sleep 1
  if [[ $i -eq 30 ]]; then
    echo "[seed] ERROR: Trino did not become ready in time" >&2
    exit 1
  fi
done

echo "[seed] Creating schema iceberg.demo ..."
timeout 30s "${TRINO_EXEC_PREFIX[@]}" --execute "CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/');" || {
  echo "[seed] ERROR: failed to create schema" >&2; exit 1; }

echo "[seed] Creating table iceberg.demo.events ..."
timeout 30s "${TRINO_EXEC_PREFIX[@]}" --execute "CREATE TABLE IF NOT EXISTS iceberg.demo.events (id bigint, ts timestamp, payload varchar);" || {
  echo "[seed] ERROR: failed to create table" >&2; exit 1; }

echo "[seed] Inserting sample rows ..."
timeout 30s "${TRINO_EXEC_PREFIX[@]}" --execute "INSERT INTO iceberg.demo.events VALUES (1, current_timestamp, 'hello'), (2, current_timestamp, 'world');" || {
  echo "[seed] ERROR: failed to insert rows" >&2; exit 1; }

echo "[seed] Verifying row count ..."
timeout 30s "${TRINO_EXEC_PREFIX[@]}" --execute "SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id FROM iceberg.demo.events;" || {
  echo "[seed] ERROR: verification query failed" >&2; exit 1; }

echo "[seed] Done."


