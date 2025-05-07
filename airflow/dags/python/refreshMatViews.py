import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    "start_date": airflow.utils.dates.days_ago(1)
}

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
            postgres_conn_id='POSTGRES_CONN'
        )

with DAG(
    'Refresh_Materialized_Views',
    default_args=default_args,
    schedule_interval=None,
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