# Apache Superset + the drivers the 4.x base image no longer ships:
#   psycopg2-binary  - metadb driver (shared local Postgres)
#   sqlalchemy-trino - Trino dialect (imported as trino.sqlalchemy)
FROM apache/superset:4.1.1

RUN pip install --no-cache-dir psycopg2-binary "sqlalchemy-trino>=0.4"
