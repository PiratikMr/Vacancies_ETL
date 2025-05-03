import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    "start_date": airflow.utils.dates.days_ago(1)
}

mat_views_pr1 =[
        'avg_sal_hh',
        'avg_sal_gj',
        'avg_sal_gm'
]

mat_views_pr2 = [
        'vacs_count',
        'avg_med_sal',
        'top_companies',
        'top_skills',
        'english_level',
        'top_fields',
        'top_grade',
        'top_experience',
        'top_employment',
        'top_schedule',
        'vacs_pday'
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
    
    refresh_tasks = []

    for view in mat_views_pr1:
        task = create_refresh_task(view)
        refresh_tasks.append(task)


    for view in mat_views_pr2:
        task = create_refresh_task(view)
        for rt in refresh_tasks:
            rt >> task