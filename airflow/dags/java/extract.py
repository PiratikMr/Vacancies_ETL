import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta

curr_date = airflow.utils.timezone.utcnow() + timedelta(hours=7)

dict = {
    "filePath":"hdfs://namenode:9000/hhapi/",
    "date":curr_date.strftime("%Y-%m-%d/"),
    "pages":"20",
    "vacancies":"100",
    "url1":"https://api.hh.ru/vacancies?page=",
    "url2":"&per_page=",
    "url3":"&host=hh.ru&professional_role=",
    "PRid":"11"
}

dag = DAG(
    dag_id = "extract",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["java"],
    schedule_interval = None
)

extract = SparkSubmitOperator(
    task_id="extract",
    conn_id="spark-conn",
    application="jobs/java_extract/target/extract-1.0.jar",
    env_vars = dict,
    java_class="Main",
    dag=dag
)

extract 