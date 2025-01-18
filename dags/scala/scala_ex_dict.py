import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta

dict = {
    "filePath":"hdfs://namenode:9000/hhapi/",
    "dictionaries":"https://api.hh.ru/dictionaries",
    "areas":"https://api.hh.ru/areas",
    "professional_roles":"https://api.hh.ru/professional_roles"
}


dag = DAG(
    dag_id = "Extract_dictionaries",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["scala"],
    schedule_interval = None
)

extract = SparkSubmitOperator(
    task_id="extract",
    conn_id="spark-conn",
    application="jobs/scala_ETL_project/extract_dict/target/scala-2.12/extract_dict-assembly-1.jar",
    dag=dag
)

extract