import pendulum
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from pyhocon import ConfigFactory
from pathlib import Path


# airflow variables
repDir = Variable.get("ITCLUSTER_HOME")
spark_binary = Variable.get("SPARK_SUBMIT")


confPath = Path(repDir) / "conf" / "config.conf"
with open(confPath, 'r') as f:
    config = ConfigFactory.parse_string(f.read())

def get(fieldName, section="Dags.deleteExpiredData"):
    return config[f"{section}.{fieldName}"]


# general
timeZone = get("TimeZone", "Dags")
hdfsPath = f'/{get("path", "FS")}'

# specific
hdfsPrefix = get("hdfsPrefix")
schedule = get("schedule")

class SiteConfig:
    def __init__(self, tag: str, trans_data_days: int, raw_data_days: int, 
                 trans_dirs=None, raw_dirs=None):
        self.tag = tag
        self.transDataDays = trans_data_days
        self.transDirs = ["Vacancies", "Skills"] + (trans_dirs or [])
        self.rawDataDays = raw_data_days
        self.rawDirs = ["RawVacancies"] + (raw_dirs or [])

siteConfs = [
    SiteConfig(tag, 
               get("trans", f"Dags.deleteExpiredData.{tag}"), 
               get("raw", f"Dags.deleteExpiredData.{tag}"), 
               trans_dirs=dirs)
    for tag, dirs in [
        ("hh", ["Employers"]),
        ("gj", ["Fields", "JobFormat", "Level", "Locations"]),
        ("gm", ["Locations"])
    ]
]


def deleteData_command(task_id, key, path):
    return f"""
        target=$(date -d "{{{{ ti.xcom_pull(task_ids='{task_id}', key='{key}') }}}}" +%s)
        {hdfsPrefix} hdfs dfs -ls {path} |
            grep '^d' |
            awk -F '{path}/' '{{print $NF}}' |
            grep -E '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' |
            while read -r dir; do
                curr=$(date -d "$dir" +%s 2>/dev/null)
                if [[ -n "$curr" && "$target" -gt "$curr" ]]; then
                    {hdfsPrefix} hdfs dfs -rm -r {path}/$dir
                fi 
            done
    """


def defineTragetsDates_task(**context):
    execution_date = context['execution_date']
    res = {}
    for sc in siteConfs:
        res.update({ f"{sc.tag}_raw" : (execution_date - timedelta(days=sc.rawDataDays)).strftime('%Y-%m-%d')})
        res.update({ f"{sc.tag}_trans" : (execution_date - timedelta(days=sc.transDataDays)).strftime('%Y-%m-%d')})
    return res


with DAG(
    dag_id="Delete_expiredDataTest",
    default_args= {
        "start_date": pendulum.instance(days_ago(1)).in_timezone(timeZone)
    },
    schedule_interval = schedule if schedule else None,
    tags=["bash"],
    concurrency = 5
) as dag:

    defineTask = PythonOperator(
        task_id = "define_target_dates",
        python_callable=defineTragetsDates_task,
        provide_context=True,
        do_xcom_push=True,
        multiple_outputs=True
    )

    
    for sc in siteConfs:
        dirPath = f'{hdfsPath}{sc.tag}/'
        for dir_type, dirs in [('raw', sc.rawDirs), ('trans', sc.transDirs)]:
            for dir in dirs:
                bash_task = BashOperator(
                    task_id=f'delete_{sc.tag}_{dir}',
                    bash_command=deleteData_command(defineTask.task_id, f"{sc.tag}_{dir_type}", f"{dirPath}{dir}")
                )
                defineTask >> bash_task