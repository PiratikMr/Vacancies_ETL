import utils
from airflow import DAG

conf = utils.Config("hh.conf")
conf.configForETL("HeadHunter/Vacancies")

with DAG(
    "HeadHunter_ETL",
    default_args = {
        "start_date": conf.startDate
    },
    schedule_interval = conf.schedule or None,
    tags = ["scala", "hh", "etl"]
) as dag:
    
    extract = conf.spark_ETLPartBuild("extract", ["--datefrom", conf.getParamFromConfFile("Dags.ETL.dateFrom")])
    transform = conf.spark_ETLPartBuild("transform")
    load = conf.spark_ETLPartBuild("load")

    extract >> transform >> load