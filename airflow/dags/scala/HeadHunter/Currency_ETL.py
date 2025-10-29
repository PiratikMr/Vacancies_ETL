import utils
from airflow import DAG

conf = utils.Config("hh.conf")
conf.configForETL("HeadHunter/Dictionaries")
app_args = ["--dictionaries", "dict:curr"]

with DAG(
    "HeadHunter_Currency_EL",
    default_args = {
        "start_date": conf.startDate
    },
    schedule_interval = conf.schedule or None,
    tags = ["scala", "hh"]
) as dag:
    
    extract = conf.spark_ETLPartBuild("extract", app_args)
    load = conf.spark_ETLPartBuild("load", app_args)

    extract >> load