create schema if not exists internal;

create or replace view internal.salary as
with calculated_salary as (
    select
        f.vacancy_id,
        case
            when f.salary_from is not null and f.salary_to is not null then 
                (f.salary_from + f.salary_to) / (2 * c.rate)
            when f.salary_from is not null then 
                f.salary_from / c.rate
            when f.salary_to is not null then 
                f.salary_to / c.rate
        end as salary,
        case
            when f.salary_from is not null and f.salary_to is not null then true
        else
            false
        end as has_range
    from fact_vacancy as f
    join dim_currency as c on c.currency_id = f.currency_id
    where (f.salary_from is not null or f.salary_to is not null)
    --   and c.is_reference = true
)
select * from calculated_salary
where salary between 1000 and 1500000;



create materialized view internal.mv_core_vacancy as
with vacancy_skills as (
    select 
        b.vacancy_id, 
        array_agg(distinct d.skill) as skills
    from bridge_vacancy_skill b
    join dim_skill d on b.skill_id = d.skill_id
    where d.is_reference = true
    group by b.vacancy_id
),
vacancy_schedules as (
    select 
        b.vacancy_id, 
        array_agg(distinct d.schedule) as schedules
    from bridge_vacancy_schedule b
    join dim_schedule d on b.schedule_id = d.schedule_id
    where d.is_reference = true
    group by b.vacancy_id
),
vacancy_locations as (
    select 
        b.vacancy_id, 
        array_agg(distinct l.location) as locations,
        array_agg(distinct c.country) as countries
    from bridge_vacancy_location b
    join dim_location l on b.location_id = l.location_id
    left join dim_country c on l.country_id = c.country_id
    where l.is_reference = true and c.is_reference = true
    group by b.vacancy_id
),
vacancy_fields as (
    select 
        b.vacancy_id, 
        array_agg(distinct d.field) as fields
    from bridge_vacancy_field b
    join dim_field d on b.field_id = d.field_id
    where d.is_reference = true
    group by b.vacancy_id
),
vacancy_grades as (
    select 
        b.vacancy_id, 
        array_agg(distinct d.grade) as grades
    from bridge_vacancy_grade b
    join dim_grade d on b.grade_id = d.grade_id
    where d.is_reference = true
    group by b.vacancy_id
),
vacancy_employments as (
    select 
        b.vacancy_id, 
        array_agg(distinct d.employment) as employments
    from bridge_vacancy_employment b
    join dim_employment d on b.employment_id = d.employment_id
    where d.is_reference = true
    group by b.vacancy_id
),
vacancy_languages as (
    select 
        b.vacancy_id, 
        array_agg(distinct l.language) as languages,
        array_agg(distinct lvl.language_level) as language_levels
    from bridge_vacancy_language b
    join dim_language l on b.language_id = l.language_id
    join dim_language_level lvl on b.language_level_id = lvl.language_level_id
    where l.is_reference = true and lvl.is_reference = true
    group by b.vacancy_id
)

select
    f.vacancy_id,
    p.platform,
    e.employer,
    c.currency,
    exp.experience,
    f.latitude,
    f.longitude,
    s.salary,
    coalesce(s.has_range, false) as has_range,
    f.published_at,
    f.title,
    f.url,
    f.closed_at,
    
    coalesce(v_sk.skills, '{}') as skills,
    coalesce(v_sch.schedules, '{}') as schedules,
    coalesce(v_loc.locations, '{}') as locations,
    coalesce(v_loc.countries, '{}') as countries,
    coalesce(v_fld.fields, '{}') as fields,
    coalesce(v_grd.grades, '{}') as grades,
    coalesce(v_emp.employments, '{}') as employments,
    coalesce(v_lng.languages, '{}') as languages,
    coalesce(v_lng.language_levels, '{}') as language_levels

from fact_vacancy f
left join dim_platform p on f.platform_id = p.platform_id
left join dim_employer e on f.employer_id = e.employer_id
left join dim_currency c on f.currency_id = c.currency_id
left join dim_experience exp on f.experience_id = exp.experience_id
left join internal.salary s on f.vacancy_id = s.vacancy_id

left join vacancy_skills v_sk on f.vacancy_id = v_sk.vacancy_id
left join vacancy_schedules v_sch on f.vacancy_id = v_sch.vacancy_id
left join vacancy_locations v_loc on f.vacancy_id = v_loc.vacancy_id
left join vacancy_fields v_fld on f.vacancy_id = v_fld.vacancy_id
left join vacancy_grades v_grd on f.vacancy_id = v_grd.vacancy_id
left join vacancy_employments v_emp on f.vacancy_id = v_emp.vacancy_id
left join vacancy_languages v_lng on f.vacancy_id = v_lng.vacancy_id
where (p.platform_id is null or p.is_reference = true)
--  and (e.employer_id is null or e.is_reference = true)
--  and (c.currency_id is null or c.is_reference = true)
  and (exp.experience_id is null or exp.is_reference = true);

create index idx_mv_core_vacancy_skills on internal.mv_core_vacancy using gin (skills);
create index idx_mv_core_vacancy_schedules on internal.mv_core_vacancy using gin (schedules);
create index idx_mv_core_vacancy_locations on internal.mv_core_vacancy using gin (locations);
create index idx_mv_core_vacancy_countries on internal.mv_core_vacancy using gin (countries);
create index idx_mv_core_vacancy_grades on internal.mv_core_vacancy using gin (grades);
create index idx_mv_core_vacancy_employments on internal.mv_core_vacancy using gin (employments);
create index idx_mv_core_vacancy_languages on internal.mv_core_vacancy using gin (languages);
create index idx_mv_core_vacancy_fields on internal.mv_core_vacancy using gin (fields);
create index idx_mv_core_vacancy_language_levels on internal.mv_core_vacancy using gin (language_levels);

create index idx_mv_core_vacancy_employer on internal.mv_core_vacancy (employer);
create index idx_mv_core_vacancy_experience on internal.mv_core_vacancy (experience);
create index idx_mv_core_vacancy_published_at on internal.mv_core_vacancy (published_at);

create index idx_mv_core_vacancy_platform on internal.mv_core_vacancy (platform);
create index idx_mv_core_vacancy_currency on internal.mv_core_vacancy (currency);
create index idx_mv_core_vacancy_salary on internal.mv_core_vacancy (salary);
create index idx_mv_core_vacancy_has_range on internal.mv_core_vacancy (has_range);

create unique index idx_mv_core_vacancy_id on internal.mv_core_vacancy (vacancy_id);


CREATE OR REPLACE FUNCTION internal.get_filtered_vacancies(
    p_from_dttm TIMESTAMP DEFAULT NULL,
    p_to_dttm TIMESTAMP DEFAULT NULL,
    p_salary_min INTEGER DEFAULT NULL,
    p_salary_max INTEGER DEFAULT NULL,
    p_has_range BOOLEAN DEFAULT NULL,
    p_platforms TEXT[] DEFAULT NULL,
    p_employers TEXT[] DEFAULT NULL,
    p_currencies TEXT[] DEFAULT NULL,
    p_experiences TEXT[] DEFAULT NULL,
    p_skills TEXT[] DEFAULT NULL,
    p_schedules TEXT[] DEFAULT NULL,
    p_locations TEXT[] DEFAULT NULL,
    p_countries TEXT[] DEFAULT NULL,
    p_fields TEXT[] DEFAULT NULL,
    p_grades TEXT[] DEFAULT NULL,
    p_employments TEXT[] DEFAULT NULL,
    p_languages TEXT[] DEFAULT NULL,
    p_language_levels TEXT[] DEFAULT NULL
) 
RETURNS TABLE (vacancy_id BIGINT) 
LANGUAGE plpgsql
STABLE PARALLEL SAFE
AS $$
DECLARE
    v_sql TEXT := 'SELECT v.vacancy_id FROM internal.mv_core_vacancy v WHERE 1=1';
BEGIN
    IF p_from_dttm IS NOT NULL THEN v_sql := v_sql || ' AND v.published_at >= $1'; END IF;
    IF p_to_dttm IS NOT NULL THEN v_sql := v_sql || ' AND v.published_at <= $2'; END IF;
    IF p_salary_min IS NOT NULL THEN v_sql := v_sql || ' AND v.salary >= $3'; END IF;
    IF p_salary_max IS NOT NULL THEN v_sql := v_sql || ' AND v.salary <= $4'; END IF;
    IF p_has_range IS NOT NULL THEN v_sql := v_sql || ' AND v.has_range = $5'; END IF;
    IF p_platforms IS NOT NULL THEN v_sql := v_sql || ' AND v.platform = ANY($6)'; END IF;
    IF p_employers IS NOT NULL THEN v_sql := v_sql || ' AND v.employer = ANY($7)'; END IF;
    IF p_currencies IS NOT NULL THEN v_sql := v_sql || ' AND v.currency = ANY($8)'; END IF;
    IF p_experiences IS NOT NULL THEN v_sql := v_sql || ' AND v.experience = ANY($9)'; END IF;
    IF p_skills IS NOT NULL THEN v_sql := v_sql || ' AND v.skills && $10'; END IF;
    IF p_schedules IS NOT NULL THEN v_sql := v_sql || ' AND v.schedules && $11'; END IF;
    IF p_locations IS NOT NULL THEN v_sql := v_sql || ' AND v.locations && $12'; END IF;
    IF p_countries IS NOT NULL THEN v_sql := v_sql || ' AND v.countries && $13'; END IF;
    IF p_fields IS NOT NULL THEN v_sql := v_sql || ' AND v.fields && $14'; END IF;
    IF p_grades IS NOT NULL THEN v_sql := v_sql || ' AND v.grades && $15'; END IF;
    IF p_employments IS NOT NULL THEN v_sql := v_sql || ' AND v.employments && $16'; END IF;
    IF p_languages IS NOT NULL THEN v_sql := v_sql || ' AND v.languages && $17'; END IF;
    IF p_language_levels IS NOT NULL THEN v_sql := v_sql || ' AND v.language_levels && $18'; END IF;

    RETURN QUERY EXECUTE v_sql USING 
        p_from_dttm, p_to_dttm, p_salary_min, p_salary_max, p_has_range,
        p_platforms, p_employers, p_currencies, p_experiences,
        p_skills, p_schedules, p_locations, p_countries, p_fields,
        p_grades, p_employments, p_languages, p_language_levels;
END;
$$;