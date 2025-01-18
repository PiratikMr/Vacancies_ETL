import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta


curr_date = airflow.utils.timezone.utcnow() + timedelta(hours=7)
dict = {
    "filePath":"hdfs://namenode:9000/hhapi/",
    "date":curr_date.strftime("%Y-%m-%d/")
    }

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
    # env_vars= dict,
    dag=dag
)

transform 