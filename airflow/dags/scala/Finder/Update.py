import sys
from airflow import DAG
from pathlib import Path
from airflow.providers.postgres.operators.postgres import PostgresOperator


common_path = str(Path(__file__).parent.parent.parent)
sys.path.append(common_path)

from config_utils import set_config, get_section_params, spark_task_build, postgres_getActiveVacancies

args = set_config("fn.conf", "Finder", "Vacancies")
dag_params = get_section_params("Dags.Update", ["schedule"])


app_args = [
    "--conffile", str(args["confPath"])
]


with DAG(
    "Finder_Update",
    default_args={
        "start_date": args["start_date"]
    },
    schedule_interval=dag_params["schedule"] or None,
    tags=["scala", "fn"]
) as dag:
    
    getCount = postgres_getActiveVacancies()
    task = spark_task_build("update", app_args + ["--activevacancies", "{{ ti.xcom_pull(task_ids='getActiveVacancies')[0][0] }}"])

    getCount >> task