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


get = lambda fieldName, section="Dags.gj": config.get_string(f"{section}.{fieldName}")

# common
scalaVersion = get("ScalaVersion", "Dags")
sparkConnId = get("SparkConnId", "Dags")

# specific
date = get("date")
pageLimit = get("pageLimit")
schedule = get("schedule")
rawPartitions = get("rawPartitions")
transformPartitions = get("transformPartitions")



args = [
    "--date", date,
    "--fileName", str(confPath)
]


jarPath = lambda etlPart: str(Path(repDir) / "jobs" / "scala_ETL_project" / "GeekJOB" / "Vacancies" / etlPart / "target" / f"scala-{scalaVersion}" / f"{etlPart}.jar")


with DAG(
    "GeekJOB_ETL",
    default_args={
        "start_date": days_ago(1)
    },
    schedule_interval = schedule if schedule else None,
    tags = ["scala", "geekJob"]
) as dag:
    
    extract = SparkSubmitOperator(
        task_id = "extract",
        conn_id = sparkConnId,
        application = jarPath("extract"),
        application_args = args + [
            "--pagelimit", pageLimit,
            "--partitions", rawPartitions
        ]
    )

    transform = SparkSubmitOperator(
        task_id = "transform",
        conn_id = sparkConnId,
        application = jarPath("transform"),
        application_args = args + [
            "--partitions", transformPartitions
        ]
    )   

    load = SparkSubmitOperator(
        task_id = "load",
        conn_id = sparkConnId,
        application = jarPath("load"),
        application_args = args
    )

    extract >> transform >> load