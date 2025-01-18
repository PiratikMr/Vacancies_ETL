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
    dag_id = "extract_dictionaries",
    tags = ["java"],
    schedule_interval = None
)

extract = SparkSubmitOperator(
    task_id="extract_dictionaries",
    conn_id="spark-conn",
    application="jobs/java_extract_dictionaries/target/extract_dictionaries-1.0.jar",
    env_vars = dict,
    java_class="Main",
    dag=dag
)

extract 