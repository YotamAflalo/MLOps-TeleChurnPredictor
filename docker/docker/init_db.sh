#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    \i /docker-entrypoint-initdb./data/init_tables.sql
    \i /docker-entrypoint-initdb.d/insert_test_data.sql
EOSQL