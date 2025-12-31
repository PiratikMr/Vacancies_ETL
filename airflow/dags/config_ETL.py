import pendulum

CONFIG_DIR_PATH = "/opt/airflow/conf"

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2025, 10, 1, 0, 0, 0, tz="Asia/Krasnoyarsk")
}


class Platform:
    def __init__(self, 
                 fileName: str,
                 moduleName: str,
                 args: list[tuple[str, str, bool]] = [],
                 u = True
                 ):
        self.fileName = f"{CONFIG_DIR_PATH}/{fileName}.conf"
        self.moduleName = moduleName
        self.args = [
            ("savefolder", "Dags.ETL.fileName", False),
            ("conffile", self.fileName, True)
        ] + args 
        self.parts = ["update"] if u else []
        self.parts.extend(["extract", "transform", "load"])
        

PLATFORMS = [
    Platform("exchangerate", "Currency", u = False),
    Platform("fn", "Finder"),
    Platform("gm", "GetMatch"),
    Platform("gj", "GeekJOB"),
    Platform("hc", "HabrCareer"),
    Platform("hh", "HeadHunterDictionaries", u = False),
    Platform("hh", "HeadHunter", args = [("datefrom", "Dags.ETL.dateFrom", False)])
]