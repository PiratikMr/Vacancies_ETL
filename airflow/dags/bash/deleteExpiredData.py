import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from pathlib import Path

common_path = str(Path(__file__).parent.parent)
sys.path.append(common_path)

from config_utils import set_config, get_section_params

args = set_config("common.conf", None, None)
dag_params = get_section_params("Dags.DeleteData", ["hdfsPrefix", "schedule", "concurrency"])
hdfsPrefix = dag_params["hdfsPrefix"]
hdfsPath = get_section_params("FS", ["path"])["path"]

sites = [
    ("hh", ["Skills", "Employers", "Languages"]),
    ("gj", ["Skills", "Fields", "JobFormats", "Levels", "Locations"]),
    ("gm", ["Skills"]),
    ("fn", ["Fields", "Locations"])
]
params = {}
for site, dirs in sites:
    set_config(f"{site}.conf", None, None)
    tmp_params = get_section_params("Dags.DeleteData", ["rawData", "transformedData"])
    params.update({ site : {
        "raw" : tmp_params["rawData"],
        "trans" : tmp_params["transformedData"]
    }})


class SiteConfig:
    def __init__(self, tag: str, trans_data_days: int, raw_data_days: int, 
                 trans_dirs=None):
        self.tag = tag
        self.transDataDays = trans_data_days
        self.transDirs = ["Vacancies", ] + (trans_dirs or [])
        self.rawDataDays = raw_data_days
        self.rawDirs = ["RawVacancies"]

siteConfs = [
    SiteConfig(tag, 
               params[tag]["raw"], 
               params[tag]["trans"], 
               trans_dirs=dirs)
    for tag, dirs in sites
]


def deleteData_command(task_id, key, path):
    return f"""
        target=$(date -d "{{{{ ti.xcom_pull(task_ids='{task_id}', key='{key}') }}}}" +%s)
        {hdfsPrefix} hdfs dfs -ls {path} |
            grep '^d' |
            awk -F '{path}/' '{{print $NF}}' |
            grep -E '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}(T[0-9]{{2}})?' |
            while read -r dir; do
                date_part=${{dir%%T*}}
                curr=$(date -d "$date_part" +%s 2>/dev/null)
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
        "start_date": args["start_date"]
    },
    tags=["bash"],
    schedule_interval = dag_params["schedule"] or None,
    concurrency = dag_params["concurrency"]
) as dag:

    defineTask = PythonOperator(
        task_id = "define_target_dates",
        python_callable=defineTragetsDates_task,
        provide_context=True,
        do_xcom_push=True,
        multiple_outputs=True
    )

    
    for sc in siteConfs:
        dirPath = f'/{hdfsPath}{sc.tag}/'
        for dir_type, dirs in [("raw", sc.rawDirs), ("trans", sc.transDirs)]:
            for dir in dirs:
                bash_task = BashOperator(
                    task_id=f'delete_{sc.tag}_{dir}',
                    bash_command=deleteData_command(defineTask.task_id, f"{sc.tag}_{dir_type}", f"{dirPath}{dir}")
                )
                defineTask >> bash_task