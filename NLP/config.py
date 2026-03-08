DB_URL = 'postgresql://postgres:1234@localhost:14432/vacstorage'
ML_MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'

def create_dict_config(display_name, base_word, threshold, ref_type=None, target_table=None, context_prefix=""):
    reference = None
    if ref_type == 'bridge':
        reference = {
            "type": "bridge",
            "table": target_table if target_table else f"bridge_vacancy_{base_word}",
            "dim_fk": f"{base_word}_id",
            "parent_fk": "vacancy_id"
        }
    elif ref_type == 'direct':
        reference = {
            "type": "direct",
            "table": target_table if target_table else "fact_vacancy",
            "dim_fk": f"{base_word}_id"
        }

    return {
        "name": display_name,
        "dim_table": f"dim_{base_word}",
        "id_col": f"{base_word}_id",
        "val_col": base_word,
        "mapping_table": f"mapping_dim_{base_word}",
        "threshold": threshold,
        "reference": reference,
        "context_prefix": context_prefix
    }

DICTIONARIES = [
    create_dict_config(
        display_name="Навыки", 
        base_word="skill", 
        threshold=0.96, 
        ref_type="bridge",
        context_prefix=""
    ),
  
    create_dict_config(
        display_name="Страны", 
        base_word="country", 
        threshold=0.90, 
        ref_type="direct", 
        target_table="dim_location"
    ),

    # create_dict_config(
    #     display_name="Локации", 
    #     base_word="location", 
    #     threshold=0.99, 
    #     ref_type="bridge"
    # ),


    create_dict_config(
        display_name="Специальности", 
        base_word="field", 
        threshold=0.96, 
        ref_type="bridge"
    ),
]