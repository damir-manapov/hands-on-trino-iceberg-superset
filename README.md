## Hands-on: Trino + Iceberg (Nessie) + MinIO + Superset

Research environment for experimenting with Trino, Iceberg (using Project Nessie as the catalog), MinIO (S3-compatible object storage), and Apache Superset for visualization.

### Stack
- **MinIO**: S3-compatible object storage (`http://localhost:9000`, console on `http://localhost:9001`)
- **Project Nessie**: Iceberg catalog (`http://localhost:19120`)
- **Trino**: Query engine (`http://localhost:8080`)
- **Superset**: BI/Visualization (`http://localhost:8088`)
- **Starter**: A no-op container that waits until all services are healthy

### Quickstart
1. Start the stack (use Docker Compose v2 command):
   ```bash
   docker compose -f compose/compose.yaml up -d
   ```

2. Wait for health checks to pass:
   - MinIO: `http://localhost:9001` (console) | Health: `http://localhost:9000/minio/health/ready`
   - Nessie: `http://localhost:19120/q/health`
   - Trino: `http://localhost:8080/v1/info`
   - Superset: `http://localhost:8088/health`

3. Access UIs:
   - MinIO console: `http://localhost:9001` — user `admin` / password `admin12345`
   - Superset: `http://localhost:8088` — user `admin` / password `admin12345`

### Trino Iceberg + Nessie configuration
Trino is configured with an Iceberg catalog that uses Nessie and stores data in MinIO. The catalog file is at:
`compose/trino/catalog/iceberg.properties`.

Key settings used:
- `iceberg.catalog.type=nessie`
- `iceberg.nessie.uri=http://nessie:19120/api/v2`
- `iceberg.s3.endpoint=http://minio:9000`
- `iceberg.s3.path-style-access=true`

### Connect Superset to Trino
Once Superset is healthy:
1. Open `http://localhost:8088` and log in (`admin` / `admin12345`).
2. Go to Settings → Data → Databases → + Database.
3. Choose “Trino” and use the URI:
   ```
   trino://trino@trino:8080
   ```
   - Username can be any non-empty value (e.g., `trino`).

### Sample SQL in Trino
Use the Trino web UI (`http://localhost:8080`) or any Trino client.
```sql
-- Create a new Iceberg schema (bucket path is optional; data lands in MinIO)
CREATE SCHEMA IF NOT EXISTS iceberg.demo;

-- Create a sample Iceberg table
CREATE TABLE IF NOT EXISTS iceberg.demo.events (
  id bigint,
  ts timestamp,
  payload varchar
);

-- Insert data
INSERT INTO iceberg.demo.events VALUES (1, current_timestamp, 'hello');

-- Query
SELECT * FROM iceberg.demo.events;
```

### Tear down
```bash
docker compose -f compose/compose.yaml down -v
```

### Notes
- Credentials in this setup are for local research only. Change them for any persistent/shared use.
- Superset here uses its container-managed SQLite metadata DB for simplicity.
- The `starter` service simply blocks after all health checks pass, ensuring `docker compose ps` shows the stack as running and ready.


