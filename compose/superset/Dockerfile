FROM apache/superset:4.1.4

# Install Trino client + SQLAlchemy dialect globally (visible to runtime)
USER root
RUN pip install --no-cache-dir \
      'trino[sqlalchemy]' \
      sqlalchemy-trino \
      lz4 zstandard \
      requests urllib3 certifi chardet idna \
      python-dateutil pytz tzlocal orjson
USER superset


