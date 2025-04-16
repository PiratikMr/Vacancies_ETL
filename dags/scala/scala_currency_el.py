import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


dag = DAG(
    dag_id = "Currency_EL",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["scala", "hh"],
    schedule_interval = None
)

extract = SparkSubmitOperator(
    task_id="extract",
    conn_id="spark-conn",
    application="jobs/scala_ETL_project/extract_currency/target/scala-2.12/extract_currency-assembly-1.jar",
    application_args = [
        
        "--fileName", "config.conf"
        
        ],
    dag=dag
)

load = SparkSubmitOperator(
    task_id="load",
    conn_id="spark-conn",
    application="jobs/scala_ETL_project/load_currency/target/scala-2.12/load_currency-assembly-1.jar",
    application_args = [

        "--fileName", "config.conf"
        
        ],
    dag=dag
)

extract >> load