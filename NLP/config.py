import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "14432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "1234")
DB_NAME = os.getenv("DB_NAME", "vacstorage")

DB_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SEMANTIC_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
DEFAULT_MATCH_THRESHOLD = 0.85
RAPIDFUZZ_WEIGHT = 0.4
SEMANTIC_WEIGHT = 0.6

DIMENSIONS_CONFIG = {
    "skill": {
        "relation_type": "bridge",
        "threshold": 0.82
    },
    "employer": {
        "relation_type": "fact",
        "threshold": 0.85
    },
    "schedule": {
        "relation_type": "bridge",
        "threshold": 0.80
    },
    "location": {
        "relation_type": "bridge",
        "threshold": 0.85
    },
    "platform": {
        "relation_type": "fact",
        "threshold": 0.90
    }
}
