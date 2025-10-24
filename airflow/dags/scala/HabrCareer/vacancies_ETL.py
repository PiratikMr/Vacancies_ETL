import sys
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from pathlib import Path

common_path = str(Path(__file__).parent.parent.parent)
sys.path.append(common_path)

from config_utils import set_config, get_section_params, spark_task_build


args = set_config("hc.conf", "HabrCareer", "Vacancies")
dag_params = get_section_params("Dags.ETL", ["fileName", "schedule"])



app_args = [
    "--filename", dag_params["fileName"],
    "--conffile", str(args["confPath"])
]


with DAG(
    "HabrCareer_ETL",
    default_args = {
        "start_date": args["start_date"]
    },
    schedule_interval = dag_params["schedule"] or None,
    tags = ["scala", "hc", "etl"]
) as dag:
    
    setAllVacanciesInActive = PostgresOperator(
        task_id=f'setAllVacanciesInActive',
        sql=f"update hc_vacancies set is_active = false where is_active is true;",
        postgres_conn_id=args["postgresConnId"]
    )
    extract = spark_task_build("extract", app_args)
    transform = spark_task_build("transform", app_args)
    load = spark_task_build("load", app_args)

    setAllVacanciesInActive >> extract >> transform >> load