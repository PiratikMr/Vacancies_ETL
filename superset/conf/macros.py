from jinja2 import pass_context

def get_sql_array(values):
    if not values:
        return "NULL"
    if not isinstance(values, list):
        values = [values]
    clean_values = [str(x).replace("'", "''") for x in values]
    return "ARRAY[" + ", ".join([f"'{x}'" for x in clean_values]) + "]"

@pass_context
def get_filtered_vacancies(context, table_alias='v'):
    filters = context.get('filter_values')
    get_filters = context.get('get_filters')
    
    params = {
        'p_from_dttm': 'NULL::timestamp',
        'p_to_dttm': 'NULL::timestamp',
        'p_salary_min': 'NULL::integer',
        'p_salary_max': 'NULL::integer',
        'p_has_range': 'NULL::boolean',
        'p_platforms': 'NULL::text[]',
        'p_employers': 'NULL::text[]',
        'p_currencies': 'NULL::text[]',
        'p_experiences': 'NULL::text[]',
        'p_skills': 'NULL::text[]',
        'p_schedules': 'NULL::text[]',
        'p_locations': 'NULL::text[]',
        'p_countries': 'NULL::text[]',
        'p_fields': 'NULL::text[]',
        'p_grades': 'NULL::text[]',
        'p_employments': 'NULL::text[]',
        'p_languages': 'NULL::text[]',
        'p_language_levels': 'NULL::text[]'
    }

    has_filters_applied = False

    published_from = context.get('from_dttm')
    published_to = context.get('to_dttm')  
    
    if published_from:
        params['p_from_dttm'] = f"'{published_from}'::timestamp"
        has_filters_applied = True
    if published_to:
        params['p_to_dttm'] = f"'{published_to}'::timestamp"
        has_filters_applied = True

    salary_has_range = filters('has_range')
    if salary_has_range:
        val = str(salary_has_range[0]).lower()
        if val in ['true', 'false']:
            params['p_has_range'] = val
            has_filters_applied = True

    salary_filters = get_filters('salary')
    if salary_filters:
        for f in salary_filters:
            val = f.get('val')
            op = f.get('op')
            try:
                int_val = int(float(val))
                if op == '>=':
                    params['p_salary_min'] = f"{int_val}::integer"
                    has_filters_applied = True
                elif op == '<=':
                    params['p_salary_max'] = f"{int_val}::integer"
                    has_filters_applied = True
            except (ValueError, TypeError):
                pass

    scalar_mappings = {
        'filter_platform': 'p_platforms',
        'filter_employer': 'p_employers',
        'filter_currency': 'p_currencies',
        'filter_experience': 'p_experiences'
    }
    
    for filter_name, param_name in scalar_mappings.items():
        vals = filters(filter_name)
        if vals:
            params[param_name] = f"{get_sql_array(vals)}::text[]"
            has_filters_applied = True

    array_mappings = {
        'filter_skill': 'p_skills', 
        'filter_schedule': 'p_schedules', 
        'filter_location': 'p_locations', 
        'filter_country': 'p_countries', 
        'filter_field': 'p_fields', 
        'filter_grade': 'p_grades', 
        'filter_employment': 'p_employments', 
        'filter_language': 'p_languages', 
        'filter_language_level': 'p_language_levels'
    }
    
    for filter_name, param_name in array_mappings.items():
        vals = filters(filter_name)
        if vals:
            params[param_name] = f"{get_sql_array(vals)}::text[]"
            has_filters_applied = True

    if not has_filters_applied:
        return ""
    
    args_list = [
        f"p_from_dttm => {params['p_from_dttm']}",
        f"p_to_dttm => {params['p_to_dttm']}",
        f"p_salary_min => {params['p_salary_min']}",
        f"p_salary_max => {params['p_salary_max']}",
        f"p_has_range => {params['p_has_range']}",
        f"p_platforms => {params['p_platforms']}",
        f"p_employers => {params['p_employers']}",
        f"p_currencies => {params['p_currencies']}",
        f"p_experiences => {params['p_experiences']}",
        f"p_skills => {params['p_skills']}",
        f"p_schedules => {params['p_schedules']}",
        f"p_locations => {params['p_locations']}",
        f"p_countries => {params['p_countries']}",
        f"p_fields => {params['p_fields']}",
        f"p_grades => {params['p_grades']}",
        f"p_employments => {params['p_employments']}",
        f"p_languages => {params['p_languages']}",
        f"p_language_levels => {params['p_language_levels']}"
    ]

    args_list_str = ",\n".join(args_list)
    func_call = f"internal.get_filtered_vacancies(\n{args_list_str}\n)".format(args_list_str=args_list_str)
    
    return f"JOIN {func_call} f ON {table_alias}.vacancy_id = f.vacancy_id"