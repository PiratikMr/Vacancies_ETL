from sqlalchemy import create_engine, text
import config

engine = create_engine(config.DB_URL)

def get_records(dimension_name: str, is_reference: bool):
    table_name = f"dim_{dimension_name}"
    id_col = f"{dimension_name}_id"
    name_col = f"{dimension_name}"
    
    with engine.connect() as conn:
        query = text(f"""
            SELECT {id_col}, {name_col} 
            FROM {table_name}
            WHERE is_reference = :is_ref
        """)
        result = conn.execute(query, {"is_ref": is_reference}).fetchall()
        return [(row[0], row[1]) for row in result]

def apply_normalization(dimension_name: str, relation_type: str, candidate_id: int, golden_id: int):
    id_col = f"{dimension_name}_id"
    dim_table = f"dim_{dimension_name}"
    mapping_table = f"mapping_dim_{dimension_name}"
    
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {mapping_table} ({id_col}, mapped_value, is_canonical)
            SELECT :g_id, mapped_value, false
            FROM {mapping_table}
            WHERE {id_col} = :c_id
            ON CONFLICT ({id_col}, mapped_value) DO NOTHING
        """), {"g_id": golden_id, "c_id": candidate_id})
        
        conn.execute(text(f"""
            DELETE FROM {mapping_table}
            WHERE {id_col} = :c_id
        """), {"c_id": candidate_id})
        
        if relation_type == "bridge":
            bridge_table = f"bridge_vacancy_{dimension_name}"
            conn.execute(text(f"""
                INSERT INTO {bridge_table} (vacancy_id, {id_col})
                SELECT vacancy_id, :g_id
                FROM {bridge_table}
                WHERE {id_col} = :c_id
                ON CONFLICT ({id_col}, vacancy_id) DO NOTHING
            """), {"g_id": golden_id, "c_id": candidate_id})
            
            conn.execute(text(f"""
                DELETE FROM {bridge_table}
                WHERE {id_col} = :c_id
            """), {"c_id": candidate_id})
            
        elif relation_type == "fact":
            conn.execute(text(f"""
                UPDATE fact_vacancy
                SET {id_col} = :g_id
                WHERE {id_col} = :c_id
            """), {"g_id": golden_id, "c_id": candidate_id})
        else:
            raise ValueError(f"Unknown relation_type: {relation_type}")
            
        conn.execute(text(f"""
            DELETE FROM {dim_table}
            WHERE {id_col} = :c_id
        """), {"c_id": candidate_id})
