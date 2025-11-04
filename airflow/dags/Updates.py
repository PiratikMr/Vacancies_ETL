import utils
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

for source, module in utils.UPDATES:

    conf = utils.Config(source)
    
    @dag(
        dag_id=f"{module}_Update",
        tags=["scala", "update", module],
        start_date=utils.DEFAULT_START_DATE,
        schedule=conf.getString("Dags.Update.schedule"),
        catchup=False
    )
    def create_dag():
        getCount = PostgresOperator(
            task_id = "getActiveVacancies",
            sql = f"select count(*) from {source}_vacancies where is_active is true;",
            postgres_conn_id = "POSTGRES_CONN"
        )

        update = conf.spark_ETLTaskBuild(
            "update",
            f"{module}/Vacancies",
            ["--activevacancies", "{{ ti.xcom_pull(task_ids='getActiveVacancies')[0][0] }}"]
        )

        getCount >> update
    
    create_dag()