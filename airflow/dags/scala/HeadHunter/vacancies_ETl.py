import sys
from airflow import DAG
from pathlib import Path

common_path = str(Path(__file__).parent.parent.parent)
sys.path.append(common_path)

from config_utils import set_config, get_section_params, spark_task_build

args = set_config("hh.conf", "HeadHunter", "Vacancies")
dag_params = get_section_params("Dags.ETL", ["fileName", "schedule", "dateFrom"])


app_args = [
    "--filename", dag_params["fileName"],
    "--conffile", str(args["confPath"])
]

with DAG(
    "HeadHunter_ETL",
    default_args={
        "start_date": args["start_date"]
    },
    schedule_interval = dag_params["schedule"] or None,
    tags = ["scala", "hh"]
) as dag:
    
    extract = spark_task_build("extract", app_args + ["--datefrom", dag_params["dateFrom"]])  
    transform = spark_task_build("transform", app_args)
    load = spark_task_build("load", app_args)

    extract >> transform >> load