{% macro create_interactive_filter_function() %}

create or replace function interactive_active.get_filters_ids(

-- vacancies
    p_sources text[] default null,

    p_published_from timestamp default null,
    p_published_to timestamp default null,

    p_salary_from double precision default null,
    p_salary_to double precision default null,
    p_salary_has_range boolean default null,
    p_currencies text[] default null,

    p_employers text[] default null,
    p_employments text[] default null,
    p_experiences text[] default null,

-- language
    p_languages text[] default null,
    p_language_levels text[] default null,

-- location
    p_location_regions text[] default null,
    p_location_countries text[] default null,

-- other
    p_fields text[] default null,
    p_grades text[] default null,
    p_schedules text[] default null,
    p_skills text[] default null

) returns table(id text) as $$

    select
        v.id as id
    from interactive_active.active_vacancies as v
    where 1 = 1
    
    -- vacancies
    and (
        p_sources is null
        or cardinality(p_sources) = 0
        or v.source = any(p_sources)
    )
    and (p_published_from is null or v.published_at >= p_published_from)
    and (p_published_to is null or v.published_at <= p_published_to)

    and (p_salary_from is null or v.salary >= p_salary_from)
    and (p_salary_to is null or v.salary <= p_salary_to)

    and (p_salary_has_range is null or v.has_range = p_salary_has_range)

    and (
        p_currencies is null
        or cardinality(p_currencies) = 0
        or v.currency = any(p_currencies)
    )

    and (
        p_employers is null
        or cardinality(p_employers) = 0
        or v.employer = any(p_employers)
    )

    and (
        p_employments is null
        or cardinality(p_employments) = 0
        or v.employment = any(p_employments)
    )

    and (
        p_experiences is null
        or cardinality(p_experiences) = 0
        or v.experience = any(p_experiences)
    )

    -- languages
    and (
        ((p_languages is null or cardinality(p_languages) = 0)
        and 
        (p_language_levels is null or cardinality(p_language_levels) = 0))

        or exists (
            select 1 from interactive_active.active_languages as l
            where l.id = v.id
            and (
                (p_languages is null or cardinality(p_languages) = 0)
                or
                l.language = any(p_languages)
            )
            and (
                (p_language_levels is null or cardinality(p_language_levels) = 0)
                or
                l.level = any(p_language_levels)
            )
        )
    )

    -- location
    and (
        ((p_location_regions is null or cardinality(p_location_regions) = 0)
        and 
        (p_location_countries is null or cardinality(p_location_countries) = 0))

        or exists (
            select 1 from interactive_active.active_locations as l
            where l.id = v.id
            and (
                (p_location_regions is null or cardinality(p_location_regions) = 0)
                or
                l.region = any(p_location_regions)
            )
            and (
                (p_location_countries is null or cardinality(p_location_countries) = 0)
                or
                l.country = any(p_location_countries)
            )
        )
    )

    -- fields
    and (
        p_fields is null
        or cardinality(p_fields) = 0
        or exists (
            select 1 from interactive_active.active_fields as f
            where f.id = v.id and f.field = any(p_fields)
        )
    )

    -- grades
    and (
        p_grades is null
        or cardinality(p_grades) = 0
        or exists (
            select 1 from interactive_active.active_grades as g
            where g.id = v.id and g.grade = any(p_grades)
        )
    )

    -- schedules
    and (
        p_schedules is null
        or cardinality(p_schedules) = 0
        or exists (
            select 1 from interactive_active.active_schedules as s
            where s.id = v.id and s.schedule = any(p_schedules)
        )
    )

    -- schedules
    and (
        p_skills is null
        or cardinality(p_skills) = 0
        or exists (
            select 1 from interactive_active.active_skills as s
            where s.id = v.id and s.skill = any(p_skills)
        )
    )

$$ language sql stable;

{% endmacro %}