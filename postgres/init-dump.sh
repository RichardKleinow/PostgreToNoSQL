#!/bin/bash
set -e


psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE DATABASE dvdrental;
	GRANT ALL PRIVILEGES ON DATABASE dvdrental TO $POSTGRES_USER;
EOSQL

pg_restore -U "$POSTGRES_USER" -d dvdrental  /data/dump/dvdrental.tar
