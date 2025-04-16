import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


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
    conn_id="spark-conn",
    application="jobs/scala_ETL_project/extract_dict/target/scala-2.12/extract_dict-assembly-1.jar",
    application_args = ["--fileName", "config.conf"],
    dag=dag
)


load = SparkSubmitOperator(
    task_id="load",
    conn_id="spark-conn",
    application="jobs/scala_ETL_project/load_dict/target/scala-2.12/load_dict-assembly-1.jar",
    application_args = ["--fileName", "config.conf"],
    dag=dag
)

extract >> load