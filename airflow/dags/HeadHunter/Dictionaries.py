import utils
from airflow.decorators import dag

hh = next((source for source in utils._SOURCES if source[0] == "hh") ,None)
conf = utils.Config(hh[0])


dictionaries_config = [
    ("Dictionaries", []),
    ("Currency", ["--dictionaries", "dict:curr"])
]


for dict, args in dictionaries_config:
    @dag(
        dag_id=f"{hh[1]}_{dict}_EL",
        tags=["scala", "etl", hh[1], dict],
        start_date=utils.DEFAULT_START_DATE,
        schedule=conf.getString(f"Dags.{dict}.schedule") or None,
        catchup=False
    )
    def create_dag():

        extract = conf.spark_ETLTaskBuild("extract", f"{hh[1]}/Dictionaries", args)
        load = conf.spark_ETLTaskBuild("load", f"{hh[1]}/Dictionaries", args)

        extract >> load

    create_dag()