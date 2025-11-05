#!/bin/sh
set -eu

TRINO_SERVER="http://trino:8080"
TRINO_CMD="trino --server $TRINO_SERVER --output-format CSV"

echo "[seed] Waiting for Trino to accept queries (timeout ~30s)..."
i=0
while :; do
  if sh -c "$TRINO_CMD --execute 'SELECT 1'" >/dev/null 2>&1; then
    echo "[seed] Trino is ready."
    break
  fi
  i=$((i+1))
  if [ $i -ge 30 ]; then
    echo "[seed] ERROR: Trino did not become ready in time" >&2
    exit 1
  fi
  sleep 1
done

echo "[seed] Creating schema iceberg.demo ..."
sh -c "$TRINO_CMD --execute 'CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location=\'s3://warehouse/demo/\');'"

echo "[seed] Creating table iceberg.demo.events ..."
sh -c "$TRINO_CMD --execute 'CREATE TABLE IF NOT EXISTS iceberg.demo.events (id bigint, ts timestamp, payload varchar);'"

echo "[seed] Inserting sample rows ..."
sh -c "$TRINO_CMD --execute 'INSERT INTO iceberg.demo.events SELECT 1, current_timestamp, \'hello\' WHERE NOT EXISTS (SELECT 1 FROM iceberg.demo.events WHERE id = 1);'"
sh -c "$TRINO_CMD --execute 'INSERT INTO iceberg.demo.events SELECT 2, current_timestamp, \'world\' WHERE NOT EXISTS (SELECT 1 FROM iceberg.demo.events WHERE id = 2);'"

echo "[seed] Verifying row count ..."
sh -c "$TRINO_CMD --execute 'SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id FROM iceberg.demo.events;'"

echo "[seed] Done."


