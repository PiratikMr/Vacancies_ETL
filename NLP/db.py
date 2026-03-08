import pandas as pd
from sqlalchemy import text

def fetch_dictionary_data(engine, config):
    query = f"SELECT {config['id_col']} as id, {config['val_col']} as val FROM {config['dim_table']}"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        return []
        
    return list(zip(df['id'], df['val']))

def execute_merges(engine, config, merges_plan):
    print("\nНачинаем процесс слияния в БД...")
    try:
        with engine.begin() as conn:
            for canonical_id, dup_ids in merges_plan.items():
                for dup_id in dup_ids:
                    ref = config.get('reference')
                    if ref:
                        if ref['type'] == 'bridge':
                            sql_insert = text(f"""
                                INSERT INTO {ref['table']} ({ref['parent_fk']}, {ref['dim_fk']})
                                SELECT {ref['parent_fk']}, :canonical_id FROM {ref['table']} WHERE {ref['dim_fk']} = :dup_id
                                ON CONFLICT ({ref['parent_fk']}, {ref['dim_fk']}) DO NOTHING;
                            """)
                            conn.execute(sql_insert, {"canonical_id": canonical_id, "dup_id": dup_id})
                            
                            sql_delete = text(f"""
                                DELETE FROM {ref['table']} WHERE {ref['dim_fk']} = :dup_id;
                            """)
                            conn.execute(sql_delete, {"dup_id": dup_id})
                            
                        elif ref['type'] == 'direct':
                            sql_update = text(f"""
                                UPDATE {ref['table']} SET {ref['dim_fk']} = :canonical_id WHERE {ref['dim_fk']} = :dup_id;
                            """)
                            conn.execute(sql_update, {"canonical_id": canonical_id, "dup_id": dup_id})

                    sql_mapping = text(f"""
                        UPDATE {config['mapping_table']} 
                        SET {config['id_col']} = :canonical_id, is_canonical = false 
                        WHERE {config['id_col']} = :dup_id;
                    """)
                    conn.execute(sql_mapping, {"canonical_id": canonical_id, "dup_id": dup_id})

                    sql_dim = text(f"DELETE FROM {config['dim_table']} WHERE {config['id_col']} = :dup_id;")
                    conn.execute(sql_dim, {"dup_id": dup_id})
                    
        print("[УСПЕХ] Все дубликаты успешно слиты, транзакция зафиксирована!")
    except Exception as e:
        print(f"[ОШИБКА] Откат (ROLLBACK). Текст ошибки: {e}")
