import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator


rootPath = "/VacStorage/"
DockerCon = "docker exec namenode"
default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
}

class SiteConfig:
    def __init__(self, tag:str, transDataDays:int, rawDataDays:int, transDirs=None, rawDirs=None):
        self.tag = tag
        self.transDataDays = transDataDays
        self.transDirs = ["Vacancies", "Skills"] + (transDirs if transDirs is not None else [])
        self.rawDataDays = rawDataDays
        self.rawDirs = ["RawVacancies"] + (rawDirs if rawDirs is not None else [])

siteConfs = [
    SiteConfig("hh", transDataDays = 30, rawDataDays = 60, transDirs=["Employers"]),
    SiteConfig("gj", 30, 60, transDirs=["Fields", "JobFormat", "Level", "Locations"]),
    SiteConfig("gm", 30, 60, transDirs=["Locations"])
]



prevTasks = []

def deleteData_command(path, task_id):
    return f"""
        target=$(date -d "{{{{ ti.xcom_pull(task_ids='{task_id}', key='return_value') }}}}" +%s)
        {DockerCon} hdfs dfs -ls {path} |
            grep '^d' |
            awk -F '{path}/' '{{print $NF}}' |
            grep -E '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' |
            while read -r dir; do
                curr=$(date -d "$dir" +%s 2>/dev/null)
                if [[ -n "$curr" && "$target" -gt "$curr" ]]; then
                    {DockerCon} hdfs dfs -rm -r {path}/$dir
                fi 
            done
    """


def defineTargetDate_task(siteConf: SiteConfig, isRaw: bool):
    days = (siteConf.rawDataDays if isRaw else siteConf.transDataDays)
    targetTag = 'Raw' if isRaw else 'Transform'
    
    defineTask = BashOperator(
        task_id = f'DefineTargetDateFor_{targetTag}Date_{siteConf.tag}',
        bash_command = f"""
            target=$(date -d "{{{{ execution_date.strftime('%Y-%m-%d') }}}} - {days} days" +%Y-%m-%d)
            echo $target
        """
    )
    for task in prevTasks:
        task >> defineTask
    prevTasks.clear()
    prevTasks.append(defineTask)
    return defineTask


def createDeleteData_tasks(siteConf:SiteConfig, dirPath:str, isRaw:bool):
    dirArr = (siteConf.rawDirs if isRaw else siteConf.transDirs)
    nameId = ('Raw' if isRaw else 'Transform')

    defineTask = prevTasks[0]
    prevTasks.clear()
    
    for dir in dirArr:
        path = f'{dirPath}{dir}'
        bash_task = BashOperator(
            task_id = f'Delete{nameId}Data_{siteConf.tag}_{dir}',
            bash_command = deleteData_command(path, defineTask.task_id)
        )
        prevTasks.append(bash_task)
        defineTask >> bash_task


with DAG(
    dag_id="Delete_expiredData",
    default_args=default_args,
    schedule_interval=None,
    tags=["bash"],
) as dag:

    for siteConf in siteConfs:
        dirPath = f'{rootPath}{siteConf.tag}/'

        defineTgTFDate = defineTargetDate_task(siteConf, False)
        defineTgTFDate

        createDeleteData_tasks(siteConf, dirPath, False)

        defineTgRawDate = defineTargetDate_task(siteConf, True)
        defineTgRawDate

        createDeleteData_tasks(siteConf, dirPath, True)     