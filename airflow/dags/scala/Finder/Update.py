import utils
from airflow import DAG

conf = utils.Config("fn.conf")
conf.configForETL("Finder/Vacancies")

with DAG(
    "Finder_Update",
    default_args = {
        "start_date": conf.startDate
    },
    schedule_interval = conf.schedule or None,
    tags = ["scala", "fn"]
) as dag:
    
    getCount = conf.postgres_getActiveVacancies()
    task = conf.spark_ETLPartBuild(
        "update",
        [
            "--activevacancies", "{{ ti.xcom_pull(task_ids='getActiveVacancies')[0][0] }}"
        ]
    )

    getCount >> task