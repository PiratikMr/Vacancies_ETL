import utils
from airflow.decorators import dag

def add_args(part, source, conf):
    if part == "extract" and source == "hh":
        return ["--datefrom", conf.getString("Dags.ETL.dateFrom")]
    return []


for source, module in utils._SOURCES:
    
    conf = utils.Config(source)

    @dag(
        dag_id=f"{module}_ETL",
        tags=["scala", "etl", module],
        start_date=utils.DEFAULT_START_DATE,
        schedule=conf.getString("Dags.ETL.schedule"),
        catchup=False
    )
    def create_dag():
        prev = None

        for part in ["extract", "transform", "load"]:
            task = conf.spark_ETLTaskBuild(part, f"{module}/Vacancies", add_args(part, source, conf))
            if prev: prev >> task
            prev = task
            
    create_dag()