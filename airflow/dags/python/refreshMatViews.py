import pendulum
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pyhocon import ConfigFactory
from pathlib import Path


# airflow variables
repDir = Variable.get("ITCLUSTER_HOME")
spark_binary = Variable.get("SPARK_SUBMIT")


confPath = Path(repDir) / "conf" / "config.conf"
with open(confPath, 'r') as f:
    config = ConfigFactory.parse_string(f.read())
get = lambda fieldName, section="Dags.refreshMatView": config.get_string(f"{section}.{fieldName}")


postgresConnId = get("PostgresConnId", "Dags")
timeZone = get("TimeZone", "Dags")

schedule = get("schedule")


mat_views = [
    # aggregate data lvl1
    [
        'avg_sal_hh',
        'avg_sal_gj',
        'avg_sal_gm',
        'grades_hh',
        'grades_gm'
    ],

    # aggregate data lvl2
    [
        'grades_count_sal_pmonths',
        'skills_count_sal'
    ],

    # number values
    [
        'vacs_count',
        'avg_med_sal',
        'vacs_pday',
    ],

    # grades
    [
        'top_grades',
        'vacs_grade_pmonths',
        'sal_grades_pmonths'
    ],

    # skills
    [
       'top_skills',
       'top_combined_skills_by2',
       'top_combined_skills_by3',
       'top_skills_by_grades',
       'top_skills_by_fields'
    ],

    [
        'top_companies',
        'english_level',
        'top_fields',
        'top_experiences',
        'top_employments',
        'top_schedules',
        'sal_pquarters',
    ]
]


def create_refresh_task(view):
    return PostgresOperator(
            task_id=f'refresh_{view}',
            sql=f"refresh materialized view {view};",
            postgres_conn_id=postgresConnId
        )

with DAG(
    'Refresh_Materialized_Views',
    default_args={
        "start_date": pendulum.instance(days_ago(1)).in_timezone(timeZone)
    },
    schedule_interval = schedule if schedule else None,
    tags = ["python"]
) as dag:
    
    prev_tasks = []
    curr_tasks = []

    for views in mat_views:
        for view in views:
            task = create_refresh_task(view)
            for pt in prev_tasks:
                pt >> task
            curr_tasks.append(task)
        prev_tasks = curr_tasks
        curr_tasks = []