import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


dag = DAG(
    dag_id = "Transform_vacancies",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["scala"],
    schedule_interval = None
)

transform = SparkSubmitOperator(
    task_id="transform",
    conn_id="spark-conn",
    application="jobs/scala_ETL_project/transform_vac/target/scala-2.12/transform_vac-assembly-1.jar",
    application_args = [
        #   the way to transform data out of date
        #   "--date", "2025-02-09", 
        "--fileName", "config.conf"],
    dag=dag
)

transform