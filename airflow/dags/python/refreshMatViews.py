import sys
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from pathlib import Path

common_path = str(Path(__file__).parent.parent.parent)
sys.path.append(common_path)

from config_utils import set_config, get_section_params

args = set_config("common.conf", None, None)
dag_params = get_section_params("Dags.RefreshMatViews", ["schedule", "concurrency"])



platforms = ['fn', 'gj', 'gm', 'hh']


mat_views = [

    # salary
    [f'salary.{platform}' for platform in platforms],

    # salary
    [f'overview.{platform}' for platform in platforms],

    # 1
    [
        'skills'
    ],

    # 2
    [
        'employers',
        'grades',
        'languages',
        'overview',
        'places',
        'skills_pair',
        'vacancies'
    ]
]


def create_refresh_task(view):
    return PostgresOperator(
            task_id=f'refresh_{view}',
            sql=f"refresh materialized view {view};",
            postgres_conn_id=args["postgresConnId"]
        )

with DAG(
    'Refresh_Materialized_Views',
    default_args = {
        "start_date": args["start_date"]
    },
    schedule_interval = dag_params["schedule"] or None,
    tags = ["python"],
    concurrency = dag_params["concurrency"] or 5,
) as dag:
    
    prevEmptyTask = None

    for idx, views in enumerate(mat_views, start=0):
        emptyTask = DummyOperator(task_id=f"empty_{idx}")

        for view in views:
            refrashTask = create_refresh_task(view)
            if prevEmptyTask is not None:
                prevEmptyTask >> refrashTask
            refrashTask >> emptyTask
        
        prevEmptyTask = emptyTask