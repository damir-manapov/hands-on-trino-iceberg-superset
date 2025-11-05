#!/usr/bin/env bash
set -euo pipefail

# Use system Python inside the container; global site-packages contain required deps
PYTHON=python

superset fab create-admin \
  --username "${ADMIN_USERNAME:-admin}" \
  --firstname "${ADMIN_FIRST_NAME:-Admin}" \
  --lastname "${ADMIN_LAST_NAME:-User}" \
  --email "${ADMIN_EMAIL:-admin@example.com}" \
  --password "${ADMIN_PASSWORD:-admin12345}" || true

superset db upgrade
superset init

"$PYTHON" /superset-init/init_db.py || true

exec superset run -h 0.0.0.0 -p 8088
