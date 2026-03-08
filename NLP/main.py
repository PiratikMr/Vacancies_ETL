from sqlalchemy import create_engine
from config import DB_URL, DICTIONARIES, ML_MODEL_NAME
from mdm_logic import load_model, process_dictionary

if __name__ == "__main__":
    
    engine = create_engine(DB_URL)
    model = load_model(ML_MODEL_NAME)

    for conf in DICTIONARIES:
        process_dictionary(engine, model, conf, apply_changes=True)