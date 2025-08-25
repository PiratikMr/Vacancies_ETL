import sys
from airflow import DAG
from pathlib import Path


common_path = str(Path(__file__).parent.parent.parent)
sys.path.append(common_path)

from config_utils import set_config, get_section_params, spark_task_build, postgres_getActiveVacancies

args = set_config("hh.conf", "HeadHunter", "Vacancies")
dag_params = get_section_params("Dags.Update", ["schedule"])


app_args = [
    "--conffile", str(args["confPath"])
]


with DAG(
    "HeadHunter_Update",
    default_args={
        "start_date": args["start_date"]
    },
    schedule_interval=None,
    tags=["scala", "hh"]
) as dag:
    
    getCount = postgres_getActiveVacancies()
    task = spark_task_build("update", app_args + ["--activevacancies", "{{ ti.xcom_pull(task_ids='getActiveVacancies')[0][0] }}"])

    getCount >> task