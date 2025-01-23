import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "Extract_vacancies",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["scala"],
    schedule_interval = None
)

extract = SparkSubmitOperator(
    task_id = "extract",
    conn_id = "spark-conn",
    application = "jobs/scala_ETL_project/extract_vac/target/scala-2.12/extract_vac-assembly-1.jar",
    application_args = ["--fid", "11", "--urlsps", "10"],
    dag=dag
)

extract 