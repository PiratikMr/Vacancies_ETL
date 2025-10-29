import utils
from airflow import DAG

conf = utils.Config("gm.conf")
conf.configForETL("GetMatch/Vacancies")

with DAG(
    "GetMatch_ETL",
    default_args = {
        "start_date": conf.startDate
    },
    schedule_interval = conf.schedule or None,
    tags = ["scala", "gm", "etl"]
) as dag:
    
    extract = conf.spark_ETLPartBuild("extract")
    transform = conf.spark_ETLPartBuild("transform")
    load = conf.spark_ETLPartBuild("load")

    extract >> transform >> load