import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


dag = DAG(
    dag_id = "Load_vacancies",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["scala"],
    schedule_interval = None
)

load = SparkSubmitOperator(
    task_id="load",
    conn_id="spark-conn",
    application="jobs/scala_ETL_project/load_vac/target/scala-2.12/load_vac-assembly-1.jar",
    application_args = [
        # the way to load data from other date
        # "--date", "2025-04-07",
        "--fileName", "config.conf"],
    dag=dag
)

load 