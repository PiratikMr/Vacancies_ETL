from config_ETL import DEFAULT_ARGS, Platform
from utils import build_spark_etl_task, get_config, parse_args
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

IDXS = [0, 1, 2]

platform_config = Platform("az", "Adzuna", u=False)
conf_tree = get_config(platform_config.fileName)

raw_schedule = conf_tree.get_string("Dags.ETL.schedule")
dag_schedule = raw_schedule if raw_schedule else None

@dag(
    dag_id="Adzuna_ETL",
    tags=["scala", "etl", "Adzuna"],
    default_args=DEFAULT_ARGS,
    schedule=dag_schedule,
    catchup=False
)
def create_dag():
    args = parse_args(conf_tree, platform_config.args)
    prev_task = None
    total_idxs = len(IDXS)

    for i, idx in enumerate(IDXS):         
        for part in platform_config.parts:
            curr_task = build_spark_etl_task(
                platform=platform_config, 
                part=part, 
                args=args + ["--locidx", str(idx)], 
                task_name=f"{part}_{idx}"
            )
            
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task
            
        if i < total_idxs - 1:
            wait_task = BashOperator(
                task_id=f"wait_after_idx_{idx}",
                bash_command="sleep 45"
            )
            prev_task >> wait_task
            prev_task = wait_task
            
create_dag()