import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DockerCon = "docker exec namenode"

class Config:
    def __init__(self, id, path, days):
        self.id = f'delete_{id}'
        self.path = f"/hhapi/{path}"
        self.days = days

configs = [
    Config("skills", "Skills", days = 30),
    Config("employers", "Employers", days = 30),
    Config("transformedVacancies", "TransformedVacancies", days = 30),
    Config("rawVacancies", "RawVacancies", days = 120),
]

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
}








def save_date(**context):
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    context['ti'].xcom_push(key='date', value=date_str)
    return date_str

def generate_bash_command(path, days, **context):
    return f"""
        execution_date_str="{{{{ ti.xcom_pull(task_ids='save_date', key='date') }}}}"
        execution_date=$(date -d "$execution_date_str" +%Y-%m-%d)
        target_date=$(date -d "$execution_date - {days} days" +%Y-%m-%d)
        target=$({DockerCon} date -d "$target_date" +%s)

        {DockerCon} hdfs dfs -ls {path} \\
            | grep '^d' \\
            | awk -F '{path}/' '{{print $NF}}' \\
            | grep -E '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' \\
            | while read -r dir; do \\
                curr=$({DockerCon} date -d "$dir" +%s 2>/dev/null)
                if [[ -n "$curr" && "$target" -gt "$curr" ]]; then
                    {DockerCon} hdfs dfs -rm -r {path}/$dir
                fi 
            done
    """

with DAG(
    dag_id="delete_data",
    default_args=default_args,
    schedule_interval=None,
    tags=["bash"],
) as dag:

    save_date_task = PythonOperator(
        task_id='save_date',
        python_callable=save_date,
        provide_context=True
    )

    for conf in configs:
        bash_task = BashOperator(
            task_id = conf.id,
            bash_command = generate_bash_command(
                path = conf.path,
                days = conf.days
            )
        )
        save_date_task >> bash_task