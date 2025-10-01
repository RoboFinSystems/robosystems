#!/bin/bash
set -e

# Create the test database if it doesn't exist
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE robosystems_test'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'robosystems_test')\gexec
EOSQL

echo "Database initialization complete. Created databases: robosystems, robosystems_test"