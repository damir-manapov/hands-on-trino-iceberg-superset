#!/bin/sh
set -eu

# Determine how to run the Trino CLI
# Preferred: host-installed `trino`. Fallbacks: `docker exec trino` or `docker run`.
TRINO_SERVER="${TRINO_SERVER:-http://trino:8080}"

if command -v trino >/dev/null 2>&1; then
  TRINO_CMD="trino --server $TRINO_SERVER --output-format CSV"
else
  if command -v docker >/dev/null 2>&1; then
    # If the running container `trino` exists, exec into it
    if docker ps --format '{{.Names}}' | grep -qx "trino"; then
      # When running inside the trino container, use localhost for the server
      TRINO_CMD="docker exec trino trino --server http://localhost:8080 --output-format CSV"
    else
      # Last resort: run a one-off CLI container on the compose network
      # Detect the compose network; default to compose_lakehouse
      DOCKER_NET="${DOCKER_NET:-compose_lakehouse}"
      TRINO_CMD="docker run --rm --network $DOCKER_NET trinodb/trino:478 trino --server http://trino:8080 --output-format CSV"
    fi
  else
    echo "[seed] ERROR: Trino CLI not found and Docker is unavailable. Install 'trino' or run via Docker." >&2
    exit 1
  fi
fi

echo "[seed] Starting seed script..." >&2
echo "[seed] TRINO_CMD: $TRINO_CMD" >&2

echo "[seed] Waiting for Trino to accept queries (timeout ~60s)..." >&2
i=0
while :; do
  if sh -c "$TRINO_CMD --execute 'SELECT 1'" >/dev/null 2>&1; then
    echo "[seed] Trino is ready." >&2
    break
  fi
  i=$((i+1))
  if [ $i -ge 60 ]; then
    echo "[seed] ERROR: Trino did not become ready in time" >&2
    exit 1
  fi
  sleep 1
done

echo "[seed] Waiting for Iceberg catalog to be available (timeout ~30s)..." >&2
i=0
while :; do
  if sh -c "$TRINO_CMD --execute 'SHOW CATALOGS'" 2>/dev/null | grep -q "iceberg"; then
    echo "[seed] Iceberg catalog is available." >&2
    break
  fi
  i=$((i+1))
  if [ $i -ge 30 ]; then
    echo "[seed] ERROR: Iceberg catalog did not become available in time" >&2
    exit 1
  fi
  sleep 1
done

# Give Trino a moment to fully stabilize after catalog is available
sleep 3

echo "[seed] Creating schema iceberg.demo ..." >&2
sh -c "$TRINO_CMD --execute 'CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='\''s3://warehouse/demo/'\'');'" || { echo "[seed] ERROR: failed to create schema" >&2; exit 1; }

echo "[seed] Creating table iceberg.demo.events ..." >&2
sh -c "$TRINO_CMD --execute 'CREATE TABLE IF NOT EXISTS iceberg.demo.events (id bigint, ts timestamp, payload varchar);'" || { echo "[seed] ERROR: failed to create table" >&2; exit 1; }

echo "[seed] Inserting sample rows ..." >&2
sh -c "$TRINO_CMD --execute 'INSERT INTO iceberg.demo.events SELECT 1, current_timestamp, '\''hello'\'' WHERE NOT EXISTS (SELECT 1 FROM iceberg.demo.events WHERE id = 1);'" || { echo "[seed] ERROR: failed to insert rows" >&2; exit 1; }
sh -c "$TRINO_CMD --execute 'INSERT INTO iceberg.demo.events SELECT 2, current_timestamp, '\''world'\'' WHERE NOT EXISTS (SELECT 1 FROM iceberg.demo.events WHERE id = 2);'" || { echo "[seed] ERROR: failed to insert rows" >&2; exit 1; }

echo "[seed] Verifying row count ..." >&2
sh -c "$TRINO_CMD --execute 'SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id FROM iceberg.demo.events;'" || { echo "[seed] ERROR: verification query failed" >&2; exit 1; }

echo "[seed] Done." >&2

