from config_ETL import DAGS_CONFIG_PATH, DEFAULT_ARGS
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils import get_config

config = get_config(DAGS_CONFIG_PATH)
dag_schedule = config.get('Dags.RefreshMatViews.schedule')

schemas = [
    "marts"
]

@dag(
    dag_id="Refresh_Materialized_views",
    default_args=DEFAULT_ARGS,
    tags=["python, postgresql"],
    schedule=dag_schedule or None,
    catchup=False
)
def create_dag():
    @task
    def get_matviews_list(schema: str):
        pg_hook = PostgresHook(postgres_conn_id="POSTGRES_CONN")
        sql = "SELECT matviewname FROM pg_matviews WHERE schemaname = %s"

        records = pg_hook.get_records(sql, parameters=(schema,))
        mv_list = [row[0] for row in records]
        return mv_list
    
    @task
    def refresh_matview(mv_name: str, schema: str):
        pg_hook = PostgresHook(postgres_conn_id="POSTGRES_CONN")
        sql = f'REFRESH MATERIALIZED VIEW {schema}.{mv_name}; ANALYZE {schema}.{mv_name};'
        pg_hook.run(sql)

    refresh_core = refresh_matview.override(task_id="refresh_internal_core_vacancy")(
        mv_name="mv_core_vacancy", 
        schema="internal"
    )

    prev = refresh_core
    
    for schema in schemas:

        @task_group(group_id=f"{schema}")
        def refresh_schema(schema: str):
            mv_list = get_matviews_list(schema)     
            refresh_matview.partial(schema=schema).expand(mv_name=mv_list)

        curr = refresh_schema(schema)
        if prev:
            prev >> curr
        prev = curr

create_dag()