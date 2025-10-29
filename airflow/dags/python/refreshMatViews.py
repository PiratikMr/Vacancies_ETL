import utils
from airflow import DAG
from airflow.operators.dummy import DummyOperator


conf = utils.Config("common.conf")

platforms = ['fn', 'gj', 'gm', 'hc', 'hh']
mat_views = [

    # salary
    [f'salary.{platform}' for platform in platforms],

    # output
    [
        'output.overview',
        'output.skills',
        'output.skills_pairs'
    ]
]

with DAG(
    'Refresh_Materialized_Views',
    default_args = {
        "start_date": conf.startDate
    },
    schedule_interval = conf.getParamFromConfFile("Dags.RefreshMatViews.schedule") or None,
    tags = ["python"],
    concurrency = 5,
) as dag:
    
    prevEmptyTask = None

    for idx, views in enumerate(mat_views, start=0):
        emptyTask = DummyOperator(task_id=f"empty_{idx}")

        for view in views:
            refrashTask = conf.postgres_refreshTask(view)
            if prevEmptyTask is not None:
                prevEmptyTask >> refrashTask
            refrashTask >> emptyTask
        
        prevEmptyTask = emptyTask