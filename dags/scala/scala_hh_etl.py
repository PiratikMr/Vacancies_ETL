import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator


dag = DAG(
    dag_id = "HH_ETL",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["scala", "hh"],
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

date = "{{ ti.xcom_pull(task_ids='save_date', key='date') }}"

extract = SparkSubmitOperator(
    task_id = "extract",
    conn_id = "spark-conn",
    application = "jobs/scala_ETL_project/extract_vac/target/scala-2.12/extract_vac-assembly-1.jar",
    application_args = [

        "--date", date,
        "--fileName", "config.conf",
        "--fid", "11",
        "--urlsps", "10"

        ],
    dag=dag
)

transform = SparkSubmitOperator(
    task_id="transform",
    conn_id="spark-conn",
    application="jobs/scala_ETL_project/transform_vac/target/scala-2.12/transform_vac-assembly-1.jar",
    application_args = [

        "--date", date, 
        "--fileName", "config.conf"

        ],
    dag=dag
)

load = SparkSubmitOperator(
    task_id="load",
    conn_id="spark-conn",
    application="jobs/scala_ETL_project/load_vac/target/scala-2.12/load_vac-assembly-1.jar",
    application_args = [

        "--date", date,
        "--fileName", "config.conf"

        ],
    dag=dag
)

save_date >> extract >> transform >> load