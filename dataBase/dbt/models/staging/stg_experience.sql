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
    from {{ source('raw_data', 'fn_vacancies') }}

    union all

    select
        concat('gj_', id) as id,
        COALESCE(experience, 'Не указан') as experience
    from {{ source('raw_data', 'gj_vacancies') }}

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
    from {{ source('raw_data', 'gm_vacancies') }}

    union all

    select
        concat('hh_', v.id) as id,
        COALESCE(e.name, 'Не указан') as experience
    from {{ source('raw_data', 'hh_vacancies') }} as v
    join {{ source('raw_data', 'hh_experience') }} as e on e.id = v.experience_id
)

select
    id,
    experience
from data