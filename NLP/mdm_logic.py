import time
from db import fetch_dictionary_data, execute_merges
from ml import load_model, get_embeddings, cluster_words
from filters import is_safe_merge, calculate_penalty

def process_dictionary(engine, model, config, apply_changes):
    print(f"\n{'='*50}\nОБРАБОТКА СЛОВАРЯ: {config['name'].upper()}\n{'='*50}")

    data = fetch_dictionary_data(engine, config)
    if not data:
        print("Словарь пуст. Пропускаем.")
        return

    words = [item[1] for item in data]
    print(f"Загружено записей: {len(data)}")

    start_time = time.time()
    prefix = config.get('context_prefix', '')
    embeddings = get_embeddings(model, words, prefix)
    print(f"Векторизация завершена за {time.time() - start_time:.2f} сек.")

    labels = cluster_words(embeddings, config['threshold'])

    clusters_dict = {}
    for word_idx, cluster_id in enumerate(labels):
        clusters_dict.setdefault(cluster_id, []).append(data[word_idx])

    duplicates_groups = [g for g in clusters_dict.values() if len(g) > 1]
    
    if not duplicates_groups:
        print("Дубликатов не найдено. Словарь чист!")
        return

    print(f"\nНАЙДЕНО ГРУПП ДУБЛИКАТОВ: {len(duplicates_groups)}\n")
    
    merges_plan = {}
    valid_group_count = 0
    
    for group in duplicates_groups:
        group.sort(key=calculate_penalty) 
        canonical = group[0]
        
        safe_duplicates = [dup for dup in group[1:] if is_safe_merge(canonical[1], dup[1])]
        
        if not safe_duplicates:
            continue
            
        merges_plan[canonical[0]] = [dup[0] for dup in safe_duplicates]
        
        valid_group_count += 1
        dup_strings = ", ".join([f"{d[1]} (id:{d[0]})" for d in safe_duplicates])
        print(f"Группа {valid_group_count}: Главное -> {canonical[1]} (id:{canonical[0]}) | Сливаем -> {dup_strings}")

    if apply_changes and merges_plan:
        confirm = input(f"\nПрименить эти {len(merges_plan)} слияний в базу для '{config['name']}'? (y/n): ")
        if confirm.lower() == 'y':
            execute_merges(engine, config, merges_plan)
        else:
            print("Слияние отменено")