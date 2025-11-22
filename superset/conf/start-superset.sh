pip install psycopg2-binary

superset fab create-admin \
               --username admin \
               --firstname Superset \
               --lastname Admin \
               --email admin@admin.com \
               --password admin
superset db upgrade
superset init

for dash in /superset-mount/dashboards/*.zip; do superset import-dashboards -p $dash -u admin; done

/usr/bin/run-server.sh