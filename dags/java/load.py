import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta


curr_date = airflow.utils.timezone.utcnow() + timedelta(hours=7)
filePath = f"hdfs://namenode:9000/hhapi/{curr_date.strftime('%Y-%m-%d/')}"
vars = {"filePath":filePath
        ,"url":"jdbc:postgresql://host.docker.internal:5432/hhapi"
        ,"user":"postgres"
        ,"password":"1234"
    }

dag = DAG(
    dag_id = "load",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["java"],
    schedule_interval = None
)

load = SparkSubmitOperator(
    task_id="load",
    conn_id="spark-conn",
    jars = "jobs/postgresql-42.7.4.jar",
    application="jobs/java_load/target/load-1.0.jar",
    env_vars = vars,
    java_class="Main",
    dag=dag
)

load 