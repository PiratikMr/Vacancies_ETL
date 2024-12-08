import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta


curr_date = airflow.utils.timezone.utcnow() + timedelta(hours=7) - timedelta(days=30)

dict = {
    "filePath":"hdfs://namenode:9000/hhapi/",
    "date":curr_date.strftime("%Y-%m-%d"),
    "minusMonths":"20"
}

dag = DAG(
    dag_id = "delete_old_data",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = None
)

delete = SparkSubmitOperator(
    task_id="deleteOldData",
    conn_id="spark-conn",
    application="jobs/java_deleteOldData/target/delete-1.0.jar",
    env_vars= dict,
    java_class="Main",
    dag=dag
)

delete 