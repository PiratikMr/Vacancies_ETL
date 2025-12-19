import pendulum
from pathlib import Path
from pyhocon import ConfigFactory
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

_confDir = "/opt/airflow/conf"
_scalaProjectDir = "/opt/airflow/scalaProject"

_SOURCES = [
    ("fn", "Finder"),
    ("gm", "GetMatch"),
    ("gj", "GeekJOB"),
    ("hc", "HabrCareer"),
    ("hh", "HeadHunter")
]

UPDATES = list(filter(lambda x: x[0] != "hc", _SOURCES))


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2025, 10, 1, 0, 0, 0, tz="Asia/Krasnoyarsk")
}


class Config:
    def __init__(self, fileConfName: str):
        self._platformTag: str = fileConfName
        self._configFilePath: str = f"{_confDir}/{fileConfName}.conf"
        self._config = ConfigFactory.parse_file(Path(self._configFilePath))
    
    def getString(self, path: str):
        return self._config.get_string(path)
    
    def spark_ETLTaskBuild(self,
                           part: str,
                           module: str,
                           args = []
                           ):
        return SparkSubmitOperator(
            task_id=part,
            conn_id="SPARK_CONN",
            application=(
                f"{_scalaProjectDir}/{module}/"
                f"target/scala-2.13/{module}-etl.jar"
            ),
            application_args=[
                "--savefolder", self.getString("Dags.ETL.fileName"),
                "--conffile", self._configFilePath,
                "--etlpart", part
            ] + args
        )