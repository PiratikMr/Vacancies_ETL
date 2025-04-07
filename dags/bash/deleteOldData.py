import airflow
from airflow import DAG
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import timedelta


dag = DAG(
    dag_id = "delete_data",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    tags = ["bash"],
    schedule_interval = None
)


days = 60
dockerCon = "docker exec namenode"
path = "/hhapi"

date = (airflow.utils.timezone.utcnow() + timedelta(hours=7) - timedelta(days=days + 1)).strftime("%Y-%m-%d")


bashCommand = f"""
target=$({dockerCon} date -d "{date}" +%s)

{dockerCon} hdfs dfs -ls {path} \
    | grep '^d' \
	| awk -F '{path}/' '{{print $NF}}' \
    | grep -E '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' \
    | while read -r dir; do \
        curr=$(date -d "$dir" +%s 2>/dev/null)
        if [[ -n "$curr" && "$target" -gt "$curr" ]]; then
            {dockerCon} hdfs dfs -rm -r {path}/$dir
        fi 
    done
"""


run_this = BashOperator(
    task_id="task",
    bash_command=bashCommand,
    dag=dag
)

run_this