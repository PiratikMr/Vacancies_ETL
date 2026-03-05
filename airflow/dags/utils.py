from pathlib import Path
from pyhocon import ConfigFactory, ConfigTree
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config_ETL import Platform

def get_config(file_path: str) -> ConfigTree:
    return ConfigFactory.parse_file(Path(file_path))

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

def build_spark_etl_task(platform: Platform, part: str, args: list[str], task_name: str = None) -> SparkSubmitOperator:
    log_conf_path = '/opt/airflow/scalaProject/Core/src/main/resources/log4j2.properties'
    
    return SparkSubmitOperator(
        task_id=task_name or part,
        conn_id="SPARK_CONN",
        application=(
            f'/opt/airflow/scalaProject/{platform.moduleName}/'
            f"target/scala-2.13/{platform.name}-etl.jar"
        ),
        files=log_conf_path,
        application_args=args + ["--etlpart", part],
        conf={
            'spark.driver.extraJavaOptions': f'-Dlog4j.configurationFile=file://{log_conf_path}',
            'spark.executor.extraJavaOptions': '-Dlog4j.configurationFile=log4j2.properties'
        }
    )