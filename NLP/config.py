import json
import os
from pathlib import Path
from threading import Lock

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_NAME = os.environ["DB_NAME"]

DB_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "5000"))

SEMANTIC_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"

SEMANTIC_WEIGHT = 0.45
TOKEN_SORT_WEIGHT = 0.25
WRATIO_WEIGHT = 0.15
JACCARD_WEIGHT = 0.15

NOISE_PREFIXES = [
    "знание", "знания", "опыт работы с", "опыт работы", "опыт",
    "навыки", "навык", "умение", "владение", "работа с", "работа в",
    "уверенное знание", "глубокое знание", "базовое знание",
    "понимание", "использование", "применение",
]

DIMENSIONS = {
    "skill":    {"label": "Навыки",       "relation_type": "bridge"},
    "employer": {"label": "Работодатели", "relation_type": "fact"},
    "schedule": {"label": "График",       "relation_type": "bridge"},
    "location": {"label": "Локации",      "relation_type": "bridge"},
    "platform": {"label": "Платформы",    "relation_type": "fact"},
    "field":    {"label": "Специальности","relation_type": "bridge"},
}

THRESHOLDS_PATH = Path(__file__).parent / "thresholds.json"
DEFAULT_THRESHOLD = 0.85
_lock = Lock()


def load_thresholds() -> dict[str, float]:
    if not THRESHOLDS_PATH.exists():
        return {name: DEFAULT_THRESHOLD for name in DIMENSIONS}
    with THRESHOLDS_PATH.open(encoding="utf-8") as f:
        data = json.load(f)
    return {name: float(data.get(name, DEFAULT_THRESHOLD)) for name in DIMENSIONS}


def save_threshold(name: str, value: float):
    if name not in DIMENSIONS:
        raise ValueError(f"Неизвестное измерение: {name}")
    with _lock:
        thresholds = load_thresholds()
        thresholds[name] = round(float(value), 4)
        with THRESHOLDS_PATH.open("w", encoding="utf-8") as f:
            json.dump(thresholds, f, ensure_ascii=False, indent=4)
