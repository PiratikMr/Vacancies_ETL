from config_ETL import PLATFORMS, DEFAULT_ARGS
from utils import spark_ETLTaskBuild, get_config, parse_args
from airflow.decorators import dag

for config in PLATFORMS:
    
    confTree = get_config(config.fileName)
    
    @dag(
        dag_id=f"{config.moduleName}_ETL",
        tags=["scala", "etl", config.moduleName],
        default_args=DEFAULT_ARGS,
        schedule=confTree.get_string("Dags.ETL.schedule") or None,
        catchup=False 
    )
    def create_dag():
        args = parse_args(confTree, config.args)
        
        prevTask = None
        for part in config.parts:
            currTask = spark_ETLTaskBuild(part, config.moduleName, args)
            if prevTask:
                prevTask >> currTask
            prevTask = currTask
    
    create_dag()