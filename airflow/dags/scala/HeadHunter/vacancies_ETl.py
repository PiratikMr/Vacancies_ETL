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
get = lambda fieldName, section="Dags.hh": config.get_string(f"{section}.{fieldName}")


# general
scalaVersion = get("ScalaVersion", "Dags")
sparkConnId = get("SparkConnId", "Dags")

# common
date = get("date")
schedule = get("schedule")

#specific
fieldId = get("fieldId")
vacsPerPage = get("vacsPerPage")
pageLimit = get("pageLimit")
urlsPerSecond = get("urlsPerSecond")
rawPartitions = get("rawPartitions")
transformPartitions = get("transformPartitions")



args = [
    "--date", date,
   "--fileName", str(confPath)
]


jarPath = lambda etlPart: str(Path(repDir) / "jobs" / "scala_ETL_project" / "HeadHunter" / "Vacancies" / etlPart / "target" / f"scala-{scalaVersion}" / f"{etlPart}.jar")


with DAG(
    "HeadHunter_ETL",
    default_args = {
        "start_date": days_ago(1)
    },
    tags = ["scala", "hh"],
    schedule_interval = schedule if schedule else None
) as dag:
    
    extract = SparkSubmitOperator(
        task_id = "extract",
        conn_id = "SPARK_CONN",
        application = jarPath("extract"),
        application_args = args + [
            "--fid", fieldId,
            "--perpage", vacsPerPage,
            "--urlsps", urlsPerSecond,
            "--pages", pageLimit,
            "--partitions", rawPartitions
        ]
    )

    transform = SparkSubmitOperator(
        task_id="transform",
        conn_id="SPARK_CONN",
        application = jarPath("transform"),
        application_args = args + [
            "--partitions", transformPartitions
        ]
    )

    load = SparkSubmitOperator(
        task_id="load",
        conn_id="SPARK_CONN",
        application = jarPath("load"),
        application_args = args
    )

    extract >> transform >> load