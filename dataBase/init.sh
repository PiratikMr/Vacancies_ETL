#!/bin/bash
set -e

user="postgres"
pass="1234"
def_db="postgres"
app_db="vacstorage"

sql="/docker-entrypoint-initdb.d/.sql"


psql -v ON_ERROR_STOP=1 --username "$user" --dbname "$def_db" <<-EOSQL
  CREATE DATABASE $app_db;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$user" --dbname "$app_db" -f "$sql"