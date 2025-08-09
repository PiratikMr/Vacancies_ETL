import sys
from airflow import DAG
from pathlib import Path


common_path = str(Path(__file__).parent.parent.parent)
sys.path.append(common_path)

from config_utils import set_config, get_section_params, spark_task_build

args = set_config("gm.conf", "GetMatch", "Vacancies")
dag_params = get_section_params("Dags.Update", ["parts", "schedule"])


app_args = [
    "--conffile", str(args["confPath"])
]


with DAG(
    "GetMatch_Update",
    default_args={
        "start_date": args["start_date"]
    },
    schedule_interval=dag_params["schedule"] or None,
    tags=["scala", "gm"]
) as dag:
    
    tasks = [
        spark_task_build("update", app_args + ["--offset", f"{i}"], f"update_part_{i}")
        for i in range(dag_params["parts"])
    ]

    for i in range(1, len(tasks)):
        tasks[i-1] >> tasks[i]