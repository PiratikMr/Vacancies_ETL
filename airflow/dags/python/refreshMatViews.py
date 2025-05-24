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

    [
        'top_grades',
        'vacs_grade_pmonths',
        'sal_grades_pmonths',
        'top_skills',
        'top_combined_skills_by2',
        'top_combined_skills_by3',
        'top_skills_by_grades',
        'top_skills_by_fields',
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
            postgres_conn_id=args["postgresConnId"]
        )

with DAG(
    'Refresh_Materialized_Views',
    default_args={
        "start_date": args["start_date"]
    },
    schedule_interval = dag_params["schedule"] or None,
    tags = ["python"],
    concurrency=dag_params["concurrency"],
) as dag:
    
    prevEmptyTask = None

    for idx, views in enumerate(mat_views, start=0):
        emptyTask = DummyOperator(task_id=f"empty{idx}")

        for view in views:
            refrashTask = create_refresh_task(view)
            if prevEmptyTask is not None:
                prevEmptyTask >> refrashTask
            refrashTask >> emptyTask
        
        prevEmptyTask = emptyTask