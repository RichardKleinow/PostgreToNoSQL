#!/bin/bash
env|sort
whoami
id


set -e

for filename in /data/dump/*.tar; do
	dbname=$(basename "$filename" .tar)
	echo "Database dump ${dbname} found"
	
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
		CREATE DATABASE "${dbname}";
		GRANT ALL PRIVILEGES ON DATABASE "${dbname}" TO $POSTGRES_USER;
	EOSQL

	pg_restore -U "$POSTGRES_USER" -d "${dbname}"  "$filename"
	echo "$filename restored to ${dbname}" 
done
