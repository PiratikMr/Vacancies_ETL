import utils
from airflow.decorators import dag

def add_args(source, conf):
    additional_args = []
    if source == "hh":
        additional_args = ["--datefrom", conf.getString("Dags.ETL.dateFrom")]
    return additional_args

for source, module in utils._SOURCES:
    
    conf = utils.Config(source)

    @dag(
        dag_id=f"{module}_ETL",
        tags=["scala", "etl", module],
        default_args=utils.DEFAULT_ARGS,
        schedule=conf.getString("Dags.ETL.schedule"),
        catchup=False
    )
    def create_dag():
        prev = None

        for part in ["update", "extract", "transform", "load"]:
            task = conf.spark_ETLTaskBuild(part, module, add_args(source, conf))
            if prev: prev >> task
            prev = task
            
    create_dag()