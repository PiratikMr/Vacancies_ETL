import os
import sys

curr_dir = os.path.dirname(os.path.abspath(__file__))
if curr_dir not in sys.path:
    sys.path.append(curr_dir)

SECRET_KEY = 'SECRET_KEY'
MAPBOX_API_KEY = 'MAPBOX_API_KEY'

HTML_SANITIZATION = False
TALISMAN_ENABLED = False

FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'ENABLE_JAVASCRIPT_CONTROLS': True
}


from macros import generate_db_function_sql

JINJA_CONTEXT_ADDONS = {
    'filter_macro': generate_db_function_sql
}


try:
    from superset_config_local import *
except ImportError:
    pass