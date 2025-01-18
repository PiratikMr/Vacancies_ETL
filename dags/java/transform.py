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
    dag_id = "transform",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["java"],
    schedule_interval = None
)

transform = SparkSubmitOperator(
    task_id="transform",
    conn_id="spark-conn",
    application="jobs/java_transform/target/transform-1.0.jar",
    env_vars= dict,
    java_class="Main",
    dag=dag
)

transform 