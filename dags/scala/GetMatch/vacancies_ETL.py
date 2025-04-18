import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator

args = [
    "--date", "{{ ti.xcom_pull(task_ids='save_date', key='date') }}",
    "--fileName", "jobs/scala_ETL_project/Configuration/config.conf",
    "--site", "gm" 
]

dag = DAG(
    dag_id = "GetMatch_ETL",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["scala", "getMatch"],
    schedule_interval = None
)

def save_date_f(**context):
    date = context['execution_date'].strftime('%Y-%m-%d')
    context['ti'].xcom_push(key='date', value=date)
    
save_date = PythonOperator(
    task_id='save_date',
    python_callable=save_date_f,
    provide_context=True,
    dag=dag,
)


extract = SparkSubmitOperator(
    task_id = "extract",
    conn_id = "spark-conn",
    application = "jobs/scala_ETL_project/GetMatch/Vacancies/extract/target/scala-2.12/extract.jar",
    application_args = args,
    dag=dag
)

transform = SparkSubmitOperator(
    task_id = "transform",
    conn_id = "spark-conn",
    application = "jobs/scala_ETL_project/GetMatch/Vacancies/transform/target/scala-2.12/transform.jar",
    application_args = args,
    dag=dag
)

load = SparkSubmitOperator(
    task_id = "load",
    conn_id = "spark-conn",
    application = "jobs/scala_ETL_project/GetMatch/Vacancies/load/target/scala-2.12/load.jar",
    application_args = args,
    dag=dag
)

save_date >> extract >> transform >> load