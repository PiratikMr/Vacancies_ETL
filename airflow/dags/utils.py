import pendulum
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pathlib import Path
from pyhocon import ConfigFactory


_confDir = "/opt/airflow/conf"
_scalaProjectDir = "/opt/airflow/scalaProject"


class Config:
    def __init__(self, fileConfName: str):

        # config file
        self._fileConfName = fileConfName
        self._configFilePath = f"{_confDir}/{fileConfName}"
        self._config = ConfigFactory.parse_file(Path(self._configFilePath))

        # airflow cons
        self._sparkConnId = self._config.get_string("Dags.SparkConnId")
        self._postgresConnId: str = self._config.get_string("Dags.PostgresConnId")

        # public vars
        self.startDate = pendulum.instance(days_ago(0)).in_timezone(self._config.get_string("Dags.TimeZone"))


    def configForETL(self, projectModuleName: str):
        # config file
        self._projectModuleName = projectModuleName
        
        # public vars
        self.schedule = self._config.get_string(f"Dags.ETL.schedule")

        # dag etl base args
        self._app_args = [
            "--filename", self._config.get_string("Dags.ETL.fileName"),
            "--conffile", self._configFilePath
        ]
      
    def getParamFromConfFile(self, path: str):
        return self._config.get_string(path)



    def _buildJarPath(self, etlPart):
        return (
            f"{_scalaProjectDir}/{self._projectModuleName}/"
            f"{etlPart}/target/scala-2.13/{etlPart}.jar"
        )
    

    def spark_ETLPartBuild(self, etlPart: str, other_args = [], task_id = None):
        if task_id is None:
            task_id = etlPart

        return SparkSubmitOperator(
            task_id = task_id,
            conn_id = self._sparkConnId,
            application = self._buildJarPath(etlPart),
            application_args = self._app_args + other_args
        )
    

    def postgres_getActiveVacancies(self):
        return PostgresOperator(
            task_id = "getActiveVacancies",
            sql = f"select count(*) from {self._fileConfName.split(".")[0]}_vacancies where is_active is true;",
            postgres_conn_id = self._postgresConnId
        )
    
    def postgres_refreshTask(self, matview):
        return PostgresOperator(
            task_id = f'refresh_{matview}',
            sql = f"refresh materialized view {matview};",
            postgres_conn_id = self._postgresConnId
        )