import os
import sys

curr_dir = os.path.dirname(os.path.abspath(__file__))
if curr_dir not in sys.path:
    sys.path.append(curr_dir)

SECRET_KEY = 'SECRET_KEY'
MAPBOX_API_KEY = 'MAPBOX_API_KEY'

SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://superset:superset@superset-postgres:5432/superset'

SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_size": 20,
    "max_overflow": 10,
    "pool_pre_ping": True,
}


SUPERSET_WEBSERVER_TIMEOUT = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300

HTML_SANITIZATION = True

HTML_SANITIZATION_SCHEMA_EXTENSIONS = {
    "attributes": {
        "span": ["style", "class", "className"],
        "a": ["href", "target", "rel"]
    },
    "tags": ["span", "a"]
}

TALISMAN_ENABLED = False

PUBLIC_ROLE_LIKE = "Gamma"

FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': False,
    'ENABLE_JAVASCRIPT_CONTROLS': True
}


from macros import get_filtered_vacancies

JINJA_CONTEXT_ADDONS = {
    'get_filtered_vacancies': get_filtered_vacancies
}


try:
    from superset_config_local import *
except ImportError:
    pass