from config_ETL import DEFAULT_ARGS, Platform
from utils import spark_ETLTaskBuild, get_config, parse_args
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

idxs = [0, 1, 2]

platformConfig = Platform("az", "Adzuna", u = False)
confTree = get_config(platformConfig.fileName)


@dag(
    dag_id=f"Adzuna_ETL",
    tags=["scala", "etl", "Adzuna"],
    default_args=DEFAULT_ARGS,
    schedule=confTree.get_string("Dags.ETL.schedule") or None,
    catchup=False 
)
def create_dag():
    args = parse_args(confTree, platformConfig.args)
    
    prevTask = None

    total_idxs = len(idxs)

    for i, idx in enumerate(idxs):         
        for part in platformConfig.parts:
            currTask = spark_ETLTaskBuild(part, platformConfig.moduleName, args + ["--locidx", f"{idx}"], task_name=f"{part}_{idx}")
            if prevTask:
                prevTask >> currTask
            prevTask = currTask
        if i < total_idxs - 1:
            wait_task = BashOperator(
                task_id=f"wait_after_idx_{idx}",
                bash_command="sleep 70"
            )
            
            prevTask >> wait_task
            prevTask = wait_task
            
create_dag() 