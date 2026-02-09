from config_ETL import Platform, DEFAULT_ARGS
from utils import spark_ETLTaskBuild, get_config, parse_args
from airflow.decorators import dag

config = Platform("hh", "HeadHunter/Dictionaries")

confTree = get_config(config.fileName)
    
@dag(
    dag_id=f"Dictionaries_ETL",
    tags=["scala", "etl", config.moduleName],
    default_args=DEFAULT_ARGS,
    schedule=confTree.get_string("Dags.ETL.schedule") or None,
    catchup=False 
)
def create_dag():
    args = parse_args(confTree, config.args)
    
    spark_ETLTaskBuild("nothing", config.moduleName, "Dictionaries", args)

create_dag()    