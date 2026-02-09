FROM apache/superset:6.0.0

USER root

RUN pip install psycopg2-binary --target /app/.venv/lib/python3.10/site-packages

RUN mkdir -p /app/superset_data && chown -R superset:superset /app/superset_data

USER superset