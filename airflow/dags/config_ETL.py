import pendulum
from pathlib import Path
from pyhocon import ConfigFactory

CONFIG_DIR_PATH  = "/opt/airflow/conf"
DAGS_CONFIG_PATH = f"{CONFIG_DIR_PATH}/dags/common.conf"

_dags_config = ConfigFactory.parse_file(Path(DAGS_CONFIG_PATH))

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2025, 10, 1, 0, 0, 0, tz="Asia/Krasnoyarsk"),
    'email': [_dags_config.get_string('Airflow.email')],
    'email_on_failure': True
}

class Platform:
    def __init__(self, 
                 fileName: str,
                 name: str,
                 args: list[tuple[str, str, bool]] = None,
                 u: bool = True,
                 module_path: str = None
                 ):
        
        if args is None:
            args = []
            
        self.fileName = f"{CONFIG_DIR_PATH}/platforms/{fileName}.conf"
        self.name = name
        self.moduleName = module_path if module_path else f"platforms/{name}"
        
        self.args = [
            ("savefolder", "Dags.ETL.fileName", False),
            ("conffile", self.fileName, True)
        ] + args 
        
        self.parts = ["update"] if u else []
        self.parts.extend(["extract", "transform-load"])
        

PLATFORMS = [
    Platform("fn", "Finder"),
    Platform("gm", "GetMatch"),
    Platform("gj", "GeekJob"),
    Platform("hc", "HabrCareer"),
    Platform("hh", "HeadHunter", args=[("datefrom", "Dags.ETL.dateFrom", False)])
]