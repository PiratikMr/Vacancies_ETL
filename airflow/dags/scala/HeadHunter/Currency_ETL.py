import sys
from airflow import DAG
from pathlib import Path

common_path = str(Path(__file__).parent.parent.parent)
sys.path.append(common_path)

from config_utils import set_config, get_section_params, spark_task_build

args = set_config("hh.conf", "HeadHunter", "Currency")
dag_params = get_section_params("Dags.Currency", ["schedule"])


app_args = [
   "--conffile", str(args["confPath"])
]

with DAG(
    "Currency_EL",
    default_args = {
        "start_date": args["start_date"]
    },
    schedule_interval = dag_params["schedule"] or None,
    tags = ["scala", "hh"]
) as dag:
    
    extract = spark_task_build("extract", app_args)
    load = spark_task_build("load", app_args)
    
    extract >> load