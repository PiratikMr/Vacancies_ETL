import utils
from airflow.decorators import dag

hh = next((source for source in utils._SOURCES if source[0] == "hh") ,None)
conf = utils.Config(hh[0])

module = f"{hh[1]}Dictionaries"


@dag(
    dag_id=f"{module}_ETL",
    tags=["scala", "etl", hh[1]],
    default_args=utils.DEFAULT_ARGS,
    schedule=conf.getString(f"Dags.Dictionaries.schedule") or None,
    catchup=False
)
def create_dag():
    prev = None

    for part in ["extract", "transform", "load"]:
        task = conf.spark_ETLTaskBuild(part, module)
        if prev: prev >> task
        prev = task

create_dag()