#!/bin/bash
set -e

# Create additional databases if they don't exist
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Test database for pytest
    SELECT 'CREATE DATABASE robosystems_test'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'robosystems_test')\gexec

    -- Dagster metadata database (separate from IAM for isolation)
    SELECT 'CREATE DATABASE dagster'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dagster')\gexec
EOSQL

echo "Database initialization complete. Created databases: robosystems, robosystems_test, dagster"