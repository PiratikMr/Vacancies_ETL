import utils
from airflow.decorators import dag

conf = utils.Config("exchangerate")
module = "Currency"

@dag(
    dag_id=f"Currency_ETL",
    tags=["scala", "etl"],
    default_args=utils.DEFAULT_ARGS,
    schedule=conf.getString("Dags.ETL.schedule") or None,
    catchup=False
)
def create_dag():
    prev = None

    for part in ["extract", "transform", "load"]:
        task = conf.spark_ETLTaskBuild(part, module)
        if prev: prev >> task
        prev = task
        
create_dag()