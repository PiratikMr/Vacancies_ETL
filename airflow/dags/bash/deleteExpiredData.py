import utils
from airflow import DAG
from airflow.operators.bash import BashOperator

conf = utils.Config("common.conf")

hdfsPath = conf.getParamFromConfFile("FS.path")

rawDataExpiresIn = conf.getParamFromConfFile("Dags.DeleteData.rawData")
transDataExpiresIn = conf.getParamFromConfFile("Dags.DeleteData.transformedData")


def deleteData_command():
    return f"""
        execution_date=$(date -d "{{{{ data_interval_end }}}}" +%Y-%m-%d)
        raw_target=$(date -d "$execution_date - {rawDataExpiresIn} days" +%s)
        trans_target=$(date -d "$execution_date - {transDataExpiresIn} days" +%s)

        hdfs ls /{hdfsPath}*/* | while read -r path; do
            if [[ $path == /* ]]; then
                root_path="${{path::-1}}"
            elif [[ $path == ????-??-??* ]]; then
                date_parts="${{path:0:10}}"
                date_ts=$(date -d "$date_parts" +%s 2>/dev/null) || continue

                [[ $root_path == */RawVacancies/* ]] && target=$raw_target || target=$trans_target

                if (( date_ts < target )); then
                    hdfs rm -r $root_path$path
                    echo "deleted $root_path$path"
                fi
            fi
        done
    """


with DAG(
    dag_id= "Delete_expiredData",
    default_args = {
        "start_date": conf.startDate
    },
    tags=["bash"],
    schedule_interval = conf.getParamFromConfFile("Dags.DeleteData.schedule") or None
) as dag:

    task = BashOperator(
        task_id = "delete",
        bash_command = deleteData_command()
    )