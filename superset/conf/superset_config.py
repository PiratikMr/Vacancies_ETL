import os
import sys

curr_dir = os.path.dirname(os.path.abspath(__file__))
if curr_dir not in sys.path:
    sys.path.append(curr_dir)

SECRET_KEY = 'SECRET_KEY'
MAPBOX_API_KEY = 'MAPBOX_API_KEY'

SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://postgres:1234@app-postgres:5432/superset_meta'

SUPERSET_WEBSERVER_TIMEOUT = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300

HTML_SANITIZATION = False
TALISMAN_ENABLED = False

PUBLIC_ROLE_LIKE = "Gamma"

FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    # 'DASHBOARD_NATIVE_FILTERS': True,
    # 'DASHBOARD_CROSS_FILTERS': False,
    'ENABLE_JAVASCRIPT_CONTROLS': True
}


from macros import apply_universal_filters

JINJA_CONTEXT_ADDONS = {
    'apply_universal_filters': apply_universal_filters
}


try:
    from superset_config_local import *
except ImportError:
    pass