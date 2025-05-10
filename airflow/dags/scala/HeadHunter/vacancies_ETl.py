import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator

args = [
    "--date", "{{ execution_date.strftime('%Y-%m-%d') }}",
   "--fileName", "conf/config.conf"
]


dag = DAG(
    dag_id = "HeadHunter_ETL",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["scala", "hh"],
    schedule_interval = None
)

   
extract = SparkSubmitOperator(
    task_id = "extract",
    conn_id = "SPARK_CONN",
    application = "jobs/scala_ETL_project/HeadHunter/Vacancies/extract/target/scala-2.12/extract.jar",
    application_args = args + [
        "--fid", "11",
        "--urlsps", "10",
        "--pages", "10"
        ],
    dag=dag
)

transform = SparkSubmitOperator(
    task_id="transform",
    conn_id="SPARK_CONN",
    application="jobs/scala_ETL_project/HeadHunter/Vacancies/transform/target/scala-2.12/transform.jar",
    application_args = args,
    dag=dag
)

load = SparkSubmitOperator(
    task_id="load",
    conn_id="SPARK_CONN",
    application="jobs/scala_ETL_project/HeadHunter/Vacancies/load/target/scala-2.12/load.jar",
    application_args = args,
    dag=dag
)

extract >> transform >> load