import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyhocon import ConfigFactory
from pathlib import Path

# airflow variables
repDir = Variable.get("ITCLUSTER_HOME")
spark_binary = Variable.get("SPARK_SUBMIT")


confPath = Path(repDir) / "conf" / "config.conf"
with open(confPath, 'r') as f:
    config = ConfigFactory.parse_string(f.read())
get = lambda fieldName, section="Dags.hh.currency": config.get_string(f"{section}.{fieldName}")


# general
scalaVersion = get("ScalaVersion", "Dags")
sparkConnId = get("SparkConnId", "Dags")
timeZone = get("TimeZone", "Dags")

# common
schedule = get("schedule")


args = [
   "--conffile", str(confPath)
]


jarPath = lambda etlPart: str(Path(repDir) / "jobs" / "scala_ETL_project" / "HeadHunter" / "Currency" / etlPart / "target" / f"scala-{scalaVersion}" / f"{etlPart}.jar")


with DAG(
    "Currency_EL",
    default_args = {
        "start_date": pendulum.instance(days_ago(1)).in_timezone(timeZone)
    },
    tags = ["scala", "hh"],
    schedule_interval = schedule if schedule else None
) as dag:
    
    extract = SparkSubmitOperator(
        task_id = "extract",
        conn_id = sparkConnId,
        application = jarPath("extract"),
        application_args = args,
        spark_binary = spark_binary
    )
    
    load = SparkSubmitOperator(
        task_id = "load",
        conn_id = sparkConnId,
        application = jarPath("load"),
        application_args = args,
        spark_binary = spark_binary
    )
    
    extract >> load