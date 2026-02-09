#!/bin/bash

MARKER_FILE="/app/superset_data/.superset_initialized"
ZIP_PATH="/superset-mount/dashboards/*.zip"

if [ ! -f "$MARKER_FILE" ]; then
    superset fab create-admin \
                --username admin \
                --firstname Superset \
                --lastname Admin \
                --email admin@admin.com \
                --password admin
    superset db upgrade
    superset init
    
    if ls $ZIP_PATH 1> /dev/null 2>&1; then
        for dash in $ZIP_PATH; do
            superset import-dashboards -p $dash -u admin;
        done
    fi

    touch "$MARKER_FILE"
fi

/usr/bin/run-server.sh