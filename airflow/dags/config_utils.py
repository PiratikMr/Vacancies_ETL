import pendulum
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pathlib import Path
from pyhocon import ConfigFactory
from airflow.models import Variable


global config
global args
global project
global module

config = None
args = None
project: str = None
module: str = None


def set_config(fileName:str, prj:str, mdl:str):
    global project, module, config, args
    
    project = prj
    module = mdl
    
    repDir = Variable.get("ITCLUSTER_HOME")
    confPath = Path(repDir) / "conf" / fileName

    config = ConfigFactory.parse_file(confPath)

    args = {
        "repDir" : repDir,
        "confPath" : confPath,
        "scalaVersion": config.get_string("Dags.ScalaVersion"),
        "sparkConnId": config.get_string("Dags.SparkConnId"),
        "postgresConnId" : config.get_string("Dags.PostgresConnId"),
        "spark_binary": Variable.get("SPARK_SUBMIT"),
        "start_date" : pendulum.instance(days_ago(1)).in_timezone(config.get_string("Dags.TimeZone"))
    }

    return args

def get_section_params(section, params):
    params = {
        par : config.get(f"{section}.{par}", None)
        for par in params
    }
    return {k: v for k, v in params.items() if v is not None}

def build_jar_path(etlPart):
    return str(Path(args["repDir"]) / "jobs" / "scala_ETL_project" / project / module / etlPart / "target" / f"scala-{args["scalaVersion"]}" / f"{etlPart}.jar")


def spark_task_build(part:str, app_args):
    return SparkSubmitOperator(
        task_id = part,
        conn_id = args["sparkConnId"],
        application = build_jar_path(part),
        application_args = app_args,
        spark_binary = args["spark_binary"]
    )