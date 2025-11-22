from jinja2 import pass_context

def get_array(values):
    if not values:
        return "NULL"
    if not isinstance(values, list):
        values = [values]
    return "array[" + ", ".join([f"'{str(x).replace(chr(39), chr(39)*2)}'" for x in values]) + "]"


@pass_context
def generate_db_function_sql(context, joinTable: str):

    filters = context.get('filter_values')
    get_filters = context.get('get_filters')

    published_from = context.get('from_dttm')
    published_to = context.get('to_dttm')  

    sources = filters('source')
    salary = get_filters('salary')
    salary_has_range = filters('has_range')
    salary_currency = filters('currency')
    employers = filters('employer')
    employments = filters('employment')
    experiences = filters('experience')
    languages = filters('language')
    language_levels = filters('level')
    countries = filters('country')
    regions = filters('region')
    fields = filters('field')
    grades = filters('grade')
    schedules = filters('schedule')
    skills = filters('skill')

    all_raw_inputs = [
        sources, published_from, published_to, salary, salary_has_range,
        salary_currency, employers, employments, experiences, languages,
        language_levels, countries, regions, fields, grades, schedules, skills
    ]

    if not any(all_raw_inputs):
        return ""


    sql_published_from = "NULL"
    if published_from:
        sql_published_from = f"'{published_from}'"

    sql_published_to = "NULL"
    if published_to:
        sql_published_to = f"'{published_to}'"


    sql_salary_from = "NULL"
    sql_salary_to = "NULL"
    for salary_filter in salary:
        val = salary_filter.get('val')
        if salary_filter.get('op') == '>=':
            sql_salary_from = val
        else:
            sql_salary_to = val


    tableName = "idsAfterFiltering"

    sql_function = f"""
        interactive_active.get_filters_ids(
            p_sources => {get_array(sources)},
            p_published_from => {sql_published_from},
            p_published_to => {sql_published_to},
            p_salary_from => {sql_salary_from},
            p_salary_to => {sql_salary_to},
            p_salary_has_range => {"NULL" if not salary_has_range else f"'{salary_has_range[0]}'"},
            p_currencies => {get_array(salary_currency)},
            p_employers => {get_array(employers)},
            p_employments => {get_array(employments)},
            p_experiences => {get_array(experiences)},
            p_languages => {get_array(languages)},
            p_language_levels => {get_array(language_levels)},
            p_location_regions => {get_array(regions)},
            p_location_countries => {get_array(countries)},
            p_fields => {get_array(fields)},
            p_grades => {get_array(grades)},
            p_schedules => {get_array(schedules)},
            p_skills => {get_array(skills)}
        ) as {tableName}
    """

    return f"JOIN {sql_function} on {tableName}.id = {joinTable}.id"
