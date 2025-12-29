from utils import get_config
from config_ETL import CONFIG_DIR_PATH, DEFAULT_ARGS
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

confTree = get_config(f"{CONFIG_DIR_PATH}/common.conf")

hdfsPath = confTree.get_string("FS.path")

rawDataExpiresIn = confTree.get_string("Dags.DeleteData.rawData")
transDataExpiresIn = confTree.get_string("Dags.DeleteData.transformedData")


def deleteData_command():
    return f"""
        execution_date=$(date -d "{{{{ logical_date }}}}" +%Y-%m-%d)
        raw_target=$(date -d "$execution_date - {rawDataExpiresIn} days" +%s)
        trans_target=$(date -d "$execution_date - {transDataExpiresIn} days" +%s)

        hdfs ls /{hdfsPath}/*/*/*/ | while read -r path; do
            if [[ $path == /* ]]; then
                root_path="${{path::-1}}"
            elif [[ $path == ????-??-??* ]]; then
                date_parts="${{path:0:10}}"
                folder_ts=$(date -d "$date_parts" +%s 2>/dev/null) || continue

                [[ $root_path == */Raw/* ]] && target=$raw_target || target=$trans_target

                if (( folder_ts < target )); then
                    hdfs rm -r $root_path$path
                    echo "deleted $root_path$path"
                fi
            fi
        done
    """

@dag(
    dag_id="Delete_expiredData",
    default_args=DEFAULT_ARGS,
    tags=["bash"],
    schedule=confTree.get_string("Dags.DeleteData.schedule") or None,
    catchup=False
)
def create_dag():
    BashOperator(
        task_id="delete",
        bash_command=deleteData_command()
    )
create_dag()