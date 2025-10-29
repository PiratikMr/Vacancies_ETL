import utils
from airflow import DAG

conf = utils.Config("hh.conf")
conf.configForETL("HeadHunter/Dictionaries")

with DAG(
    "HeadHunter_Dictionaries_EL",
    default_args = {
        "start_date": conf.startDate
    },
    schedule_interval = conf.schedule or None,
    tags = ["scala", "hh"]
) as dag:
    
    extract = conf.spark_ETLPartBuild("extract")
    load = conf.spark_ETLPartBuild("load")

    extract >> load