import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

args = [
   "--fileName", "conf/config.conf"
]

dag = DAG(
    dag_id = "Dictionaries_EL",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["scala", "hh"],
    schedule_interval = None
)

extract = SparkSubmitOperator(
    task_id="extract",
    conn_id="SPARK_CONN",
    application="jobs/scala_ETL_project/HeadHunter/Dictionaries/extract/target/scala-2.12/extract.jar",
    application_args = args,
    dag=dag
)

load = SparkSubmitOperator(
    task_id="load",
    conn_id="SPARK_CONN",
    application="jobs/scala_ETL_project/HeadHunter/Dictionaries/load/target/scala-2.12/load.jar",
    application_args = args,
    dag=dag
)

extract >> load