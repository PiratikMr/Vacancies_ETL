from config_ETL import Platform

from pathlib import Path
from pyhocon import ConfigFactory, ConfigTree
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def get_config(filePath: str) -> ConfigTree:
    return ConfigFactory.parse_file(Path(filePath))

def parse_args(config: ConfigTree, args: list[tuple[str, str, bool]]) -> list[str]:
    cli_args = []
    
    for arg_name, arg_value, is_static in args:
        cli_args.append(f"--{arg_name}")
        
        if is_static:
            cli_args.append(arg_value)
        else:
            value = config.get(arg_value)
            cli_args.append(str(value))
        
    return cli_args

def spark_ETLTaskBuild(part: str, moduleName: str, args: list[str], task_name: str = None):
    return SparkSubmitOperator(
        task_id=task_name or part,
        conn_id="SPARK_CONN",
        application=(
            f'/opt/airflow/scalaProject/{moduleName}/'
            f"target/scala-2.13/{moduleName}-etl.jar"
        ),
        application_args=args + ["--etlpart", part]
    )