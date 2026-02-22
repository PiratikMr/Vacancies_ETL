from config_ETL import PLATFORMS, DEFAULT_ARGS, Platform
from utils import spark_ETLTaskBuild, get_config, parse_args
from airflow.decorators import dag

config = Platform("currency", "Currency")

confTree = get_config(config.fileName)
    
@dag(
    dag_id="Currency_Updater",
    tags=["scala"],
    default_args=DEFAULT_ARGS,
    schedule=confTree.get_string("Dags.ETL.schedule") or None,
    catchup=False 
)
def create_dag():    
    args = parse_args(confTree, config.args)

    spark_ETLTaskBuild("part", "Currency", "Currency", args, "task")

create_dag()