from utils import get_config
from config_ETL import CONFIG_DIR_PATH, DEFAULT_ARGS
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

conf_tree = get_config(f"{CONFIG_DIR_PATH}/common.conf")

hdfs_path = conf_tree.get_string("FileSystem.path")
raw_data_expires_in = conf_tree.get_string("Dags.DeleteData.rawData")
trans_data_expires_in = conf_tree.get_string("Dags.DeleteData.transformedData")

def get_delete_data_command() -> str:
    return f"""
        execution_date=$(date -d "{{{{ ds }}}}" +%Y-%m-%d)
        raw_target=$(date -d "$execution_date - {raw_data_expires_in} days" +%s)
        trans_target=$(date -d "$execution_date - {trans_data_expires_in} days" +%s)

        hdfs ls /{hdfs_path}/*/*/*/*/ | while read -r path; do
            if [[ $path == /* ]]; then
                root_path="${{path::-1}}"
            elif [[ $path == ????-??-??* ]]; then
                date_parts="${{path:0:10}}"
                folder_ts=$(date -d "$date_parts" +%s 2>/dev/null) || continue

                [[ $root_path == */Raw/* ]] && target=$raw_target || target=$trans_target

                if (( folder_ts < target )); then
                    hdfs rm -r "$root_path$path"
                    echo "deleted $root_path$path"
                fi
            fi
        done
    """

@dag(
    dag_id="Delete_expiredData",
    default_args=DEFAULT_ARGS,
    tags=["bash", "hdfs", "cleanup"],
    schedule=conf_tree.get_string("Dags.DeleteData.schedule"),
    catchup=False
)
def create_dag():
    BashOperator(
        task_id="delete_expired_directories",
        bash_command=get_delete_data_command()
    )

create_dag()