import utils
from airflow import DAG

conf = utils.Config("hh.conf")
conf.configForETL("HeadHunter/Vacancies")

with DAG(
    "HeadHunter_Update",
    default_args = {
        "start_date": conf.startDate
    },
    schedule_interval = conf.schedule or None,
    tags = ["scala", "hh"]
) as dag:
    
    getCount = conf.postgres_getActiveVacancies()
    task = conf.spark_ETLPartBuild(
        "update",
        [
            "--activevacancies", "{{ ti.xcom_pull(task_ids='getActiveVacancies')[0][0] }}"
        ]
    )

    getCount >> task