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

# Drop and create events table with more rows
echo "[seed] Dropping table iceberg.demo.events if exists ..." >&2
sh -c "$TRINO_CMD --execute 'DROP TABLE IF EXISTS iceberg.demo.events;'" || true

echo "[seed] Creating table iceberg.demo.events ..." >&2
sh -c "$TRINO_CMD --execute 'CREATE TABLE iceberg.demo.events (id bigint, ts timestamp, event_type varchar, payload varchar, user_id bigint, amount double);'" || { echo "[seed] ERROR: failed to create table" >&2; exit 1; }

echo "[seed] Inserting 200 rows into events ..." >&2
sh -c "$TRINO_CMD --execute 'INSERT INTO iceberg.demo.events SELECT n AS id, current_timestamp - INTERVAL '\''1'\'' DAY * (n % 30), CASE (n % 5) WHEN 0 THEN '\''click'\'' WHEN 1 THEN '\''purchase'\'' WHEN 2 THEN '\''view'\'' WHEN 3 THEN '\''login'\'' ELSE '\''logout'\'' END, '\''Event '\'' || CAST(n AS varchar), (n % 50) + 1, (n * 10.5) + 100.0 FROM UNNEST(sequence(1, 200)) AS t(n);'" || { echo "[seed] ERROR: failed to insert events" >&2; exit 1; }

# Drop and create users table
echo "[seed] Dropping table iceberg.demo.users if exists ..." >&2
sh -c "$TRINO_CMD --execute 'DROP TABLE IF EXISTS iceberg.demo.users;'" || true

echo "[seed] Creating table iceberg.demo.users ..." >&2
sh -c "$TRINO_CMD --execute 'CREATE TABLE iceberg.demo.users (user_id bigint, username varchar, email varchar, created_at timestamp, country varchar, active boolean);'" || { echo "[seed] ERROR: failed to create users table" >&2; exit 1; }

echo "[seed] Inserting 50 rows into users ..." >&2
sh -c "$TRINO_CMD --execute 'INSERT INTO iceberg.demo.users SELECT n AS user_id, '\''user'\'' || CAST(n AS varchar), '\''user'\'' || CAST(n AS varchar) || '\''@example.com'\'', current_timestamp - INTERVAL '\''1'\'' DAY * (n % 90), CASE (n % 10) WHEN 0 THEN '\''US'\'' WHEN 1 THEN '\''UK'\'' WHEN 2 THEN '\''DE'\'' WHEN 3 THEN '\''FR'\'' WHEN 4 THEN '\''CA'\'' WHEN 5 THEN '\''AU'\'' WHEN 6 THEN '\''JP'\'' WHEN 7 THEN '\''BR'\'' WHEN 8 THEN '\''IN'\'' ELSE '\''MX'\'' END, n % 3 != 0 FROM UNNEST(sequence(1, 50)) AS t(n);'" || { echo "[seed] ERROR: failed to insert users" >&2; exit 1; }

# Drop and create orders table
echo "[seed] Dropping table iceberg.demo.orders if exists ..." >&2
sh -c "$TRINO_CMD --execute 'DROP TABLE IF EXISTS iceberg.demo.orders;'" || true

echo "[seed] Creating table iceberg.demo.orders ..." >&2
sh -c "$TRINO_CMD --execute 'CREATE TABLE iceberg.demo.orders (order_id bigint, user_id bigint, product_id bigint, quantity integer, price double, order_date timestamp, status varchar);'" || { echo "[seed] ERROR: failed to create orders table" >&2; exit 1; }

echo "[seed] Inserting 150 rows into orders ..." >&2
sh -c "$TRINO_CMD --execute 'INSERT INTO iceberg.demo.orders SELECT n AS order_id, (n % 50) + 1, (n % 20) + 1, (n % 5) + 1, (n * 15.75) + 50.0, current_timestamp - INTERVAL '\''1'\'' DAY * (n % 60), CASE (n % 4) WHEN 0 THEN '\''completed'\'' WHEN 1 THEN '\''pending'\'' WHEN 2 THEN '\''shipped'\'' ELSE '\''cancelled'\'' END FROM UNNEST(sequence(1, 150)) AS t(n);'" || { echo "[seed] ERROR: failed to insert orders" >&2; exit 1; }

# Drop and create products table
echo "[seed] Dropping table iceberg.demo.products if exists ..." >&2
sh -c "$TRINO_CMD --execute 'DROP TABLE IF EXISTS iceberg.demo.products;'" || true

echo "[seed] Creating table iceberg.demo.products ..." >&2
sh -c "$TRINO_CMD --execute 'CREATE TABLE iceberg.demo.products (product_id bigint, name varchar, category varchar, price double, stock integer, created_at timestamp);'" || { echo "[seed] ERROR: failed to create products table" >&2; exit 1; }

echo "[seed] Inserting 20 rows into products ..." >&2
sh -c "$TRINO_CMD --execute 'INSERT INTO iceberg.demo.products SELECT n AS product_id, '\''Product '\'' || CAST(n AS varchar), CASE (n % 5) WHEN 0 THEN '\''Electronics'\'' WHEN 1 THEN '\''Clothing'\'' WHEN 2 THEN '\''Books'\'' WHEN 3 THEN '\''Home'\'' ELSE '\''Sports'\'' END, (n * 25.50) + 10.0, (n * 10) + 50, current_timestamp - INTERVAL '\''1'\'' DAY * n FROM UNNEST(sequence(1, 20)) AS t(n);'" || { echo "[seed] ERROR: failed to insert products" >&2; exit 1; }

# Drop and create transactions table
echo "[seed] Dropping table iceberg.demo.transactions if exists ..." >&2
sh -c "$TRINO_CMD --execute 'DROP TABLE IF EXISTS iceberg.demo.transactions;'" || true

echo "[seed] Creating table iceberg.demo.transactions ..." >&2
sh -c "$TRINO_CMD --execute 'CREATE TABLE iceberg.demo.transactions (transaction_id bigint, user_id bigint, amount double, currency varchar, transaction_type varchar, timestamp timestamp, status varchar);'" || { echo "[seed] ERROR: failed to create transactions table" >&2; exit 1; }

echo "[seed] Inserting 300 rows into transactions ..." >&2
sh -c "$TRINO_CMD --execute 'INSERT INTO iceberg.demo.transactions SELECT n AS transaction_id, (n % 50) + 1, (n * 12.34) + 5.0, CASE (n % 3) WHEN 0 THEN '\''USD'\'' WHEN 1 THEN '\''EUR'\'' ELSE '\''GBP'\'' END, CASE (n % 3) WHEN 0 THEN '\''payment'\'' WHEN 1 THEN '\''refund'\'' ELSE '\''transfer'\'' END, current_timestamp - INTERVAL '\''1'\'' HOUR * (n % 720), CASE (n % 10) WHEN 0 THEN '\''failed'\'' ELSE '\''success'\'' END FROM UNNEST(sequence(1, 300)) AS t(n);'" || { echo "[seed] ERROR: failed to insert transactions" >&2; exit 1; }

echo "[seed] Verifying row counts ..." >&2
sh -c "$TRINO_CMD --execute 'SELECT '\''events'\'' AS table_name, count(*) AS rows FROM iceberg.demo.events UNION ALL SELECT '\''users'\'', count(*) FROM iceberg.demo.users UNION ALL SELECT '\''orders'\'', count(*) FROM iceberg.demo.orders UNION ALL SELECT '\''products'\'', count(*) FROM iceberg.demo.products UNION ALL SELECT '\''transactions'\'', count(*) FROM iceberg.demo.transactions ORDER BY table_name;'" || { echo "[seed] ERROR: verification query failed" >&2; exit 1; }

echo "[seed] Done." >&2

