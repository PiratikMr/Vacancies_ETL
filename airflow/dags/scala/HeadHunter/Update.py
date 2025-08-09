import sys
from airflow import DAG
from pathlib import Path


common_path = str(Path(__file__).parent.parent.parent)
sys.path.append(common_path)

from config_utils import set_config, get_section_params, spark_task_build

args = set_config("hh.conf", "HeadHunter", "Vacancies")
dag_params = get_section_params("Dags.Update", ["parts"])


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
    
    tasks = [
        spark_task_build("update", app_args + ["--offset", f"{i}"], f"update_part_{i}")
        for i in range(dag_params["parts"])
    ]

    for i in range(1, len(tasks)):
        tasks[i-1] >> tasks[i]