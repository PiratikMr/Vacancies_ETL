#!/bin/bash
set -e

user="${POSTGRES_USER}"
pass="${POSTGRES_PASSWORD}"
def_db="${POSTGRES_DB}"
app_db="vacstorage"

sql_dir="/docker-entrypoint-initdb.d/_sql"

psql -v ON_ERROR_STOP=1 --username "$user" --dbname "$def_db" <<-EOSQL
  CREATE DATABASE $app_db;
EOSQL

find "$sql_dir" -type f -name "*.sql" | sort -V | while read file; do
  echo "--> $file"
  psql -v ON_ERROR_STOP=1 --username "$user" --dbname "$app_db" -f "$file"
done