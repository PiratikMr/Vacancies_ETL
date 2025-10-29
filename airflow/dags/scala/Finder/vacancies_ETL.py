import utils
from airflow import DAG

conf = utils.Config("fn.conf")
conf.configForETL("Finder/Vacancies")

with DAG(
    "Finder_ETL",
    default_args = {
        "start_date": conf.startDate
    },
    schedule_interval = conf.schedule or None,
    tags = ["scala", "fn", "etl"]
) as dag:
    
    extract = conf.spark_ETLPartBuild("extract")
    transform = conf.spark_ETLPartBuild("transform")
    load = conf.spark_ETLPartBuild("load")

    extract >> transform >> load