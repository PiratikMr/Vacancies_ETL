import utils
from airflow import DAG

conf = utils.Config("gj.conf")
conf.configForETL("GeekJOB/Vacancies")

with DAG(
    "GeekJOB_Update",
    default_args = {
        "start_date": conf.startDate
    },
    schedule_interval = conf.schedule or None,
    tags = ["scala", "gj"]
) as dag:
    
    getCount = conf.postgres_getActiveVacancies()
    task = conf.spark_ETLPartBuild(
        "update",
        [
            "--activevacancies", "{{ ti.xcom_pull(task_ids='getActiveVacancies')[0][0] }}"
        ]
    )

    getCount >> task