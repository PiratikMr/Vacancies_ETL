import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyhocon import ConfigFactory
from pathlib import Path


repDir = os.getenv("ITCLUSTER_HOME")
confPath = Path(repDir) / "conf" / "config.conf"
with open(confPath, 'r') as f:
    config = ConfigFactory.parse_string(f.read())
get = lambda fieldName, section="Dags.hh.currency": config.get_string(f"{section}.{fieldName}")


# general
scalaVersion = get("ScalaVersion", "Dags")
sparkConnId = get("SparkConnId", "Dags")

# common
schedule = get("schedule")



args = [
   "--fileName", str(confPath)
]


jarPath = lambda etlPart: str(Path(repDir) / "jobs" / "scala_ETL_project" / "HeadHunter" / "Currency" / etlPart / "target" / f"scala-{scalaVersion}" / f"{etlPart}.jar")


with DAG(
    "Currency_EL",
    default_args = {
        "start_date": days_ago(1)
    },
    tags = ["scala", "hh"],
    schedule_interval = schedule if schedule else None
) as dag:
    
    extract = SparkSubmitOperator(
        task_id = "extract",
        conn_id = sparkConnId,
        application = jarPath("extract"),
        application_args = args
    )
    
    load = SparkSubmitOperator(
        task_id = "load",
        conn_id = sparkConnId,
        application = jarPath("load"),
        application_args = args
    )
    
    extract >> load