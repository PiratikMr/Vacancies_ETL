import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

# Импортируем вашу логику
from config import DB_URL, DICTIONARIES, ML_MODEL_NAME
from ml import load_model, get_embeddings, cluster_words
from db import fetch_dictionary_data, execute_merges
from filters import is_safe_merge, calculate_penalty

st.set_page_config(page_title="MDM Очистка словарей", layout="wide")

@st.cache_resource
def init_system():
    engine = create_engine(DB_URL)
    model = load_model(ML_MODEL_NAME)
    return engine, model

engine, model = init_system()

st.title("Интерактивное слияние дубликатов")

# === БЛОК ВЫВОДА СТАТУСА ПОСЛЕ ПЕРЕЗАГРУЗКИ СТРАНИЦЫ ===
# Если в сессии есть сообщение об успехе или ошибке, выводим его и сразу удаляем
if 'status_msg' in st.session_state:
    st.success(st.session_state['status_msg'], icon="✅")
    del st.session_state['status_msg']
if 'error_msg' in st.session_state:
    st.error(st.session_state['error_msg'], icon="🚨")
    del st.session_state['error_msg']
# ========================================================

dict_map = {d['name']: d for d in DICTIONARIES}
selected_dict = st.sidebar.selectbox("Выберите словарь для обработки:", list(dict_map.keys()))
config = dict_map[selected_dict]

threshold = st.sidebar.slider(
    "Порог схожести", 
    min_value=0.50, 
    max_value=1.00, 
    value=config['threshold'], 
    step=0.01
)

if st.sidebar.button("Найти дубликаты", type="primary"):
    with st.spinner("Извлечение данных и кластеризация..."):
        data = fetch_dictionary_data(engine, config)
        if not data:
            st.warning("Словарь пуст.")
        else:
            words = [item[1] for item in data]
            prefix = config.get('context_prefix', '')
            
            embeddings = get_embeddings(model, words, prefix)
            labels = cluster_words(embeddings, threshold)

            clusters_dict = {}
            for word_idx, cluster_id in enumerate(labels):
                clusters_dict.setdefault(cluster_id, []).append(data[word_idx])

            duplicates_groups = [g for g in clusters_dict.values() if len(g) > 1]
            
            table_data = []
            for group in duplicates_groups:
                group.sort(key=calculate_penalty) 
                canonical = group[0]
                safe_duplicates = [dup for dup in group[1:] if is_safe_merge(canonical[1], dup[1])]
                
                for dup in safe_duplicates:
                    table_data.append({
                        "Одобрить": True,
                        "Главное слово": canonical[1],
                        "Дубликат": dup[1],
                        "canonical_id": canonical[0],
                        "dup_id": dup[0]
                    })
            
            st.session_state['table_data'] = table_data
            st.session_state['config'] = config
            
            if 'editor_key' in st.session_state:
                del st.session_state['editor_key']


def set_all_checkboxes(value: bool):
    """Обновляет все строки в данных и сбрасывает ручные изменения таблицы"""
    if 'table_data' in st.session_state:
        for row in st.session_state['table_data']:
            row["Одобрить"] = value
    if 'editor_key' in st.session_state:
        del st.session_state['editor_key']


if 'table_data' in st.session_state and st.session_state['table_data']:
    total_pairs = len(st.session_state['table_data'])
    st.subheader(f"Кандидаты на слияние: {total_pairs} пар")
    
    col1, col2, _ = st.columns([2, 2, 8])
    with col1:
        st.button("✅ Выбрать все", on_click=set_all_checkboxes, args=(True,), use_container_width=True)
    with col2:
        st.button("❌ Снять все", on_click=set_all_checkboxes, args=(False,), use_container_width=True)
        
    df = pd.DataFrame(st.session_state['table_data'])
    
    edited_df = st.data_editor(
        df,
        key="editor_key", 
        column_config={
            "Одобрить": st.column_config.CheckboxColumn(
                "Одобрить", 
                help="Снимите галочку, если эти слова НЕ нужно сливать",
                default=True
            ),
            "Главное слово": st.column_config.TextColumn("Главное слово", disabled=True),
            "Дубликат": st.column_config.TextColumn("Дубликат", disabled=True),
            "canonical_id": None, 
            "dup_id": None        
        },
        hide_index=True,
        width="stretch", 
        height=600       
    )
    
    st.divider()
    
    # === ДИНАМИЧЕСКИЙ ПОДСЧЕТ ВЫБРАННЫХ СЛОВ ===
    approved_count = int(edited_df["Одобрить"].sum())
    st.info(f"📊 Выбрано дубликатов для слияния: **{approved_count}** из **{total_pairs}**")
    
    if st.button("Применить выбранные слияния в БД", type="primary"):
        approved_merges = edited_df[edited_df["Одобрить"] == True]
        
        merges_plan = {}
        for _, row in approved_merges.iterrows():
            c_id = row['canonical_id']
            d_id = row['dup_id']
            merges_plan.setdefault(c_id, []).append(d_id)
            
        if merges_plan:
            try:
                with st.spinner("Запись изменений в базу данных..."):
                    execute_merges(engine, st.session_state['config'], merges_plan)
                
                # Подсчитываем точное количество измененных слов и групп для красивого вывода
                words_merged = sum(len(dups) for dups in merges_plan.values())
                groups_count = len(merges_plan)
                
                # Сохраняем сообщение об успехе в сессию ПЕРЕД перезагрузкой
                st.session_state['status_msg'] = f"Успешно обработано: {words_merged} дубликатов слито в {groups_count} групп(ы)!"
                
                # Очищаем таблицу
                st.session_state['table_data'] = []
                if 'editor_key' in st.session_state:
                    del st.session_state['editor_key']
                
                # Перезагружаем страницу, чтобы таблица исчезла, а статус появился
                st.rerun()
                
            except Exception as e:
                # В случае сбоя БД тоже показываем пользователю причину
                st.session_state['error_msg'] = f"Произошла ошибка базы данных: {e}"
                st.rerun()
        else:
            st.warning("Вы не выбрали ни одного слова для слияния.")