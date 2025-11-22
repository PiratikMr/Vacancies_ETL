create or replace view staging.vacancies as
    with data as (
        select
            concat('fn_', id) as id,
            is_active,
            'Finder' as source,
            published_at
        from fn_vacancies

        union all

        select
            concat('gj_', id) as id,
            is_active,
            'GeekJob' as source,
            published_at
        from gj_vacancies

        union all

        select
            concat('gm_', id) as id,
            is_active,
            'GetMatch' as source,
            published_at
        from gm_vacancies

        union all

        select
            concat('hc_', id) as id,
            is_active,
            'HabrCareer' as source,
            published_at
        from hc_vacancies

        union all

        select
            concat('hh_', id) as id,
            is_active,
            'HeadHunter' as source,
            published_at
        from hh_vacancies
    ) select
        id,
        is_active,
        source,
        published_at
    from data;

-- < 5, 10 вакансий
create or replace view staging.employers as
    with data as (
        select
            concat('fn_', id) as id,
            employer
        from fn_vacancies

        union all

        select
            concat('gj_', id) as id,
            employer
        from gj_vacancies

        union all

        select
            concat('gm_', id) as id,
            employer
        from gm_vacancies

        union all

        select
            concat('hc_', id) as id,
            employer
        from hc_vacancies

        union all

        select
            concat('hh_', v.id) as id,
            e.name as employer
        from hh_vacancies as v
        join hh_employers as e on e.id = v.employer_id
    ) select
        id,
        employer
    from data
    where employer is not null;

create or replace view staging.employments as
    with data as (
        select
            concat('fn_',id) as id,
            case
                when employment_type = 'full_time' then 'Полная занятость'
                when employment_type = 'internship' then 'Стажировка'
                when employment_type = 'part_time' then 'Частичная занятость'
                when employment_type = 'project' then 'Проектная работа'
                when employment_type = 'non_standard' then '???????'
            end as employment
        from fn_vacancies

        union all

        select
            concat('hc_',id) as id,
            case
                when employment_type = 'full_time' then 'Полная занятость'
                when employment_type = 'part_time' then 'Частичная занятость'
            end as employment
        from hc_vacancies
        where employment_type is not null

        union all

        select
            concat('hh_', v.id) as id,
            e.name as employment
        from hh_vacancies as v
        join hh_employment as e on e.id = v.employment_id
    ) select
        id,
        employment
    from data;

create or replace view staging.experiences as
    with data as (
        select
            concat('fn_', id) as id,
            case
                when experience = 'three_years_more' then 'Более 3 лет'
                when experience = 'no_experience' then 'Нет опыта'
                when experience = 'up_to_one_year' then 'Менее 1 года'
                when experience = 'two_years_more' then 'Более 2 лет'
                when experience = 'five_years_more' then 'Более 5 лет'
                when experience = 'year_minimum' then 'От 1 года'
            else
                'Не указан'
            end as experience
        from fn_vacancies

        union all

        select
            concat('gj_', id) as id,
            COALESCE(experience, 'Не указан') as experience
        from gj_vacancies

        union all

        select
            concat('gm_', id) as id,
            case
                when experience_years < 1 then 'Нет опыта'
                when experience_years between 1 and 3 then 'От 1 года до 3 лет'
                when experience_years between 3 and 6 then 'От 3 до 6 лет'
                when experience_years > 6 then 'Более 6 лет'
            else
                'Не указан'
            end as experience
        from gm_vacancies

        union all

        select
            concat('hh_', v.id) as id,
            COALESCE(e.name, 'Не указан') as experience
        from hh_vacancies as v
        join hh_experience as e on e.id = v.experience_id
    ) select
        id,
        experience
    from data;

create or replace view staging.fields as
    with data as (
        select
            concat('fn_',v.id) as id,
            f.name as field
        from fn_vacancies as v
        left join fn_fields as f on f.id = v.id

        union all

        select
            concat('gj_',v.id) as id,
            f.name as field
        from gj_vacancies as v
        left join gj_fields as f on f.id = v.id

        union all

        select
            concat('hc_',v.id) as id,
            f.name as field
        from hc_vacancies as v
        left join hc_fields as f on f.id = v.id

        union all

        select
            concat('hh_',v.id) as id,
            f.name as field
        from hh_vacancies as v
        left join hh_professionalroles as f on f.id = v.role_id
    ) select
        id,
        field
    from data
    where field is not null;

create or replace view staging.grades as
    with data as (
        select
            concat('fn_', id) as id,
            case
                when lower(title) ~ '.*(intern|стаж).*' then 'Стажер'
                when lower(title) ~ '.*(jun|джун).*' then 'Джуниор'
                when lower(title) ~ '.*(midd|мидд).*' then 'Миддл'
                when lower(title) ~ '.*(sen|сеньор).*' then 'Сеньор'
            end as grade
        from fn_vacancies

        union all

        select
            concat('gj_', v.id) as id,
            g.name as grade
        from gj_vacancies as v
        join gj_grades as g on g.id = v.id

        union all

        select
            concat('gm_', id) as id,
            case
                when level = 'Senior' then 'Сеньор'
                when level = 'Lead' then 'Тимлид/Руководитель группы'
                when level = 'Middle' then 'Миддл'
                when level = 'Middle-to-Senior' then 'Миддл'
                when level = 'C-level' then 'Руководитель отдела/подразделения'
                when level = 'Junior' then 'Джуниор'
            end as grade
        from gm_vacancies

        union all

        select
            concat('hc_', id) as id,
            case
                when grade = 'Младший (Junior)' then 'Джуниор'
                when grade = 'Ведущий (Lead)' then 'Тимлид/Руководитель группы'
                when grade = 'Средний (Middle)' then 'Миддл'
                when grade = 'Стажёр (Intern)' then 'Стажер'
                when grade = 'Старший (Senior)' then 'Сеньор'
            end as grade
        from hc_vacancies

        union all

        select
            concat('hh_', id) as id,
            case
                when lower(title) ~ '.*(intern|стаж).*' then 'Стажер'
                when lower(title) ~ '.*(jun|джун).*' then 'Джуниор'
                when lower(title) ~ '.*(midd|мидд).*' then 'Миддл'
                when lower(title) ~ '.*(sen|сеньор).*' then 'Сеньор'
            end as grade
        from hh_vacancies
    ) select
        id,
        grade
    from data
    where grade is not null;

create or replace view staging.languages as
    with data as (
        select
            concat('gm_', id) as id,
            'Английский' as language,
            case
                when english_level like '%A1%' then 'A1 — Начальный'
                when english_level like '%A2%' then 'A2 — Элементарный'
                when english_level like '%B1%' then 'B1 — Средний'
                when english_level like '%B2%' then 'B2 — Средне-продвинутый'
                when english_level like '%C1%' then 'C1 — Продвинутый'
            else
                'Любой'
            end as level
        from gm_vacancies

        union all

        select
            concat('hh_', v.id) as id,
            l.name as language,
            l.level as level
        from hh_vacancies as v
        join hh_languages as l on l.id = v.id
    ) select
        id,
        language,
        level
    from data;

create or replace view staging.locations as
    with data as (
        select
            concat('fn_', v.id) as id,
            l.country as region,
            l.name as country,
            v.address_lat as lat,
            v.address_lng as lng
        from fn_vacancies as v
        join fn_locations as l on l.id = v.id

        union all

        select
            concat('gj_', v.id) as id,
            concat('Типа город ',l.name) as region,
            concat('Типа страна ',l.name) as contry,
            null as lat,
            null as lng
        from gj_vacancies as v
        join gj_locations as l on l.id = v.id

        union all

        select
            concat('hc_', v.id) as id,
            l.name as region,
            'Россия' as contry,
            null as lat,
            null as lng
        from hc_vacancies as v
        join hc_locations as l on l.id = v.id

        union all

        select
            concat('hh_', v.id) as id,
            ar.name as region,
            ac.name as contry,
            v.address_lat as lat,
            v.address_lng as lng
        from hh_vacancies as v
        join hh_areas as ar on ar.id = v.area_id
        join hh_areas as ac on ac.id = ar.parent_id
    ) select
        id,
        region,
        country,
        lat,
        lng
    from data;

create or replace view staging.salaries as 
    with data as (
        select
            concat('fn_', id) as id,
            salary_from,
            salary_to,
            salary_currency_id
        from fn_vacancies

        union all

        select
            concat('gj_', id) as id,
            salary_from,
            salary_to,
            salary_currency_id
        from gj_vacancies

        union all

        select
            concat('gm_', id) as id,
            salary_from,
            salary_to,
            salary_currency_id
        from gm_vacancies

        union all

        select
            concat('hc_', id) as id,
            salary_from,
            salary_to,
            salary_currency_id
        from hc_vacancies

        union all

        select
            concat('hh_', id) as id,
            salary_from,
            salary_to,
            salary_currency_id
        from hh_vacancies
    ), logic as (
        select
            v.id,
            case
                when v.salary_from is not null and v.salary_to is not null then 
                    (v.salary_from + v.salary_to) / (2 * c.rate)
                when v.salary_from is not null then 
                    v.salary_from / c.rate
                when v.salary_to is not null then 
                    v.salary_to / c.rate
            end as salary,
            case
                when v.salary_from is not null and v.salary_to is not null then true
            else
                false
            end as has_range,
            c.id as currency
        from data as v
        join currency as c on c.id = salary_currency_id
        where v.salary_from is not null or v.salary_to is not null
    ) select 
        id,
        salary,
        has_range,
        currency
    from logic
    where salary between 1000 and 1000000;

create or replace view staging.schedules as
    with data as (
        select
            concat('fn_', id) as id,
            'Удаленная работа' as type
        from fn_vacancies
        where distant_work is true

        union all

        select
            concat('gj_', v.id) as id,
            jf.name as type
        from gj_vacancies as v
        join gj_jobformats as jf on jf.id = v.id

        union all

        select
            concat('gm_', id) as id,
            'Удаленная работа' as type
        from gm_vacancies
        where remote_options is not null

        union all

        select
            concat('gm_', id) as id,
            'Работа в офисе' as type
        from gm_vacancies
        where office_options is not null

        union all

        select
            concat('hc_', id) as id,
            'Удаленная работа' as type
        from hc_vacancies
        where remote_work is true

        union all

        select
            concat('hh_', v.id) as id,
            sc.name as type
        from hh_vacancies as v
        join hh_schedule as sc on sc.id = v.schedule_id
    ) select
        id,
        type
    from data
    where type is not null;

create or replace view staging.skills as
    with data as (
        select
            concat('gj_', v.id) as id,
            s.name as skill
        from gj_vacancies as v
        join gj_skills as s on s.id = v.id

        union all

        select
            concat('gm_', v.id) as id,
            s.name as skill
        from gm_vacancies as v
        join gm_skills as s on s.id = v.id

        union all

        select
            concat('hc_', v.id) as id,
            s.name as skill
        from hc_vacancies as v
        join hc_skills as s on s.id = v.id

        union all

        select
            concat('hh_', v.id) as id,
            s.name as skill
        from hh_vacancies as v
        join hh_skills as s on s.id = v.id
    ) select
        id,
        skill
    from data;