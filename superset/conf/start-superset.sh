pip install psycopg2-binary
superset fab create-admin \
               --username admin \
               --firstname Superset \
               --lastname Admin \
               --email admin@admin.com \
               --password admin
superset db upgrade
superset init

superset import-dashboards -p /app/superset_export/dashboard_vacs.zip -u admin

/usr/bin/run-server.sh