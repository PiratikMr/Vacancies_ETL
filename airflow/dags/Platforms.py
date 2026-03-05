from config_ETL import PLATFORMS, DEFAULT_ARGS
from utils import build_spark_etl_task, get_config, parse_args
from airflow.decorators import dag

def generate_platform_dag(platform_cfg):
    conf_tree = get_config(platform_cfg.fileName)
    
    raw_schedule = conf_tree.get_string("Dags.ETL.schedule")
    dag_schedule = raw_schedule if raw_schedule else None
    
    @dag(
        dag_id=f"{platform_cfg.name}_ETL",
        tags=["scala", "etl", platform_cfg.moduleName],
        default_args=DEFAULT_ARGS,
        schedule=dag_schedule,
        catchup=False 
    )
    def platform_dag():
        args = parse_args(conf_tree, platform_cfg.args)
        
        prev_task = None
        for part in platform_cfg.parts:
            curr_task = build_spark_etl_task(platform=platform_cfg, part=part, args=args)
            if prev_task:
                prev_task >> curr_task
            prev_task = curr_task
            
    return platform_dag()

for platform_config in PLATFORMS:
    dag_instance = generate_platform_dag(platform_config)
    globals()[dag_instance.dag_id] = dag_instance