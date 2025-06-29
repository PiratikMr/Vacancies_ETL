pip install psycopg2-binary
superset fab create-admin \
               --username admin \
               --firstname Superset \
               --lastname Admin \
               --email admin@admin.com \
               --password admin
superset db upgrade
superset load_examples
superset init
/usr/bin/run-server.sh