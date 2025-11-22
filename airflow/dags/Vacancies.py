import utils
from airflow.decorators import dag

def add_args(part, source, conf):
    if part == "extract" and source == "hh":
        return ["--datefrom", conf.getString("Dags.ETL.dateFrom")]
    return []

# 1. Список источников
for source, module in utils._SOURCES:
    
    # Загрузка конфигураций из соответсвующего файла
    conf = utils.Config(source)

    # 2. Функция для создания DAG
    @dag(
        dag_id=f"{module}_ETL",
        tags=["scala", "etl", module],
        default_args=utils.DEFAULT_ARGS,
        schedule=conf.getString("Dags.ETL.schedule"),
        catchup=False
    )
    def create_dag():
        prev = None

        # 3. Создание линейной цепочки задач
        for part in ["extract", "transform", "load"]:
            task = conf.spark_ETLTaskBuild(part, f"{module}/Vacancies", add_args(part, source, conf))
            if prev: prev >> task
            prev = task
            
    create_dag()