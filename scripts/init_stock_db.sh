#!/bin/bash
# Runs automatically by PostgreSQL on first container start (docker-entrypoint-initdb.d).
# Creates the stock_db database and seeds it with the pipeline schema.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE stock_db;
    GRANT ALL PRIVILEGES ON DATABASE stock_db TO $POSTGRES_USER;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "stock_db" \
    -f /docker-entrypoint-initdb.d/01_schema.sql

echo "stock_db initialised with pipeline schema."
