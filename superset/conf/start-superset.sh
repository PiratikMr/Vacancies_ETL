#!/bin/bash

MARKER_FILE="/app/superset_data/.superset_initialized"
ZIP_PATH="/superset-mount/dashboards/*.zip"

if [ ! -f "$MARKER_FILE" ]; then
    superset fab create-admin \
                --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
                --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Admin}" \
                --lastname "${SUPERSET_ADMIN_LASTNAME:-User}" \
                --email "${SUPERSET_ADMIN_EMAIL:-admin@superset.local}" \
                --password "${SUPERSET_ADMIN_PASSWORD:-admin}"
    superset db upgrade
    superset init
    
    if ls $ZIP_PATH 1> /dev/null 2>&1; then
        for dash in $ZIP_PATH; do
            superset import-dashboards -p $dash -u "${SUPERSET_ADMIN_USERNAME:-admin}";
        done
    fi

    touch "$MARKER_FILE"
fi

/usr/bin/run-server.sh