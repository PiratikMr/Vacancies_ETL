from jinja2 import pass_context

def get_sql_array(values):
    if not values:
        return "NULL"
    if not isinstance(values, list):
        values = [values]
    clean_values = [str(x).replace("'", "''") for x in values]
    return "ARRAY[" + ", ".join([f"'{x}'" for x in clean_values]) + "]"

def get_in_clause(values):
    if not values:
        return "NULL"
    if not isinstance(values, list):
        values = [values]
    clean_values = [str(x).replace("'", "''") for x in values]
    return ", ".join([f"'{x}'" for x in clean_values])

@pass_context
def apply_universal_filters(context):
    filters = context.get('filter_values')
    get_filters = context.get('get_filters')
    
    where_clauses = []

    published_from = context.get('from_dttm')
    published_to = context.get('to_dttm')  
    
    if published_from:
        where_clauses.append(f"published_at >= '{published_from}'")
    if published_to:
        where_clauses.append(f"published_at <= '{published_to}'")

    salary_filters = get_filters('salary')
    if salary_filters:
        for f in salary_filters:
            val = f.get('val')
            op = f.get('op')
            if op == '>=':
                where_clauses.append(f"salary >= {val}")
            elif op == '<=':
                where_clauses.append(f"salary <= {val}")

    salary_has_range = filters('has_range')
    if salary_has_range:
        val = str(salary_has_range[0]).lower()
        where_clauses.append(f"has_range = {val}")

    # Скалярные колонки (названия совпадают: фильтр platform -> колонка platform)
    scalar_columns = [
        'platform', 
        'employer', 
        'currency', 
        'experience'
    ]
    
    for col in scalar_columns:
        vals = filters(col)
        if vals:
            where_clauses.append(f"{col} IN ({get_in_clause(vals)})")

    # Маппинг массивов: 'имя_фильтра_из_dim_таблицы': 'название_колонки_с_массивом_в_БД'
    array_mappings = {
        'skill': 'skills', 
        'schedule': 'schedules', 
        'location': 'locations', 
        'country': 'countries', 
        'field': 'fields', 
        'grade': 'grades', 
        'employment': 'employments', 
        'language': 'languages', 
        'language_level': 'language_levels'
    }
    
    for filter_name, db_column in array_mappings.items():
        vals = filters(filter_name)
        if vals:
            # Теперь Superset передает 'grade', а скрипт подставляет 'grades && ARRAY[...]'
            where_clauses.append(f"{db_column} @> {get_sql_array(vals)}")

    if not where_clauses:
        return ""

    return " AND " + " AND ".join(where_clauses)