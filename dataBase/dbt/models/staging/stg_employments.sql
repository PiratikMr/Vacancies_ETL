with data as (
    select
        concat('fn_',id) as id,
        case
            when employment_type = 'full_time' then 'Полная занятость'
            when employment_type = 'internship' then 'Стажировка'
            when employment_type = 'part_time' then 'Частичная занятость'
            when employment_type = 'project' then 'Проектная работа'
            when employment_type = 'non_standard' then 'Волонтерство'
        end as employment
    from {{ source('raw_data', 'fn_vacancies') }}

    union all

    select
        concat('hc_',id) as id,
        case
            when employment_type = 'full_time' then 'Полная занятость'
            when employment_type = 'part_time' then 'Частичная занятость'
        end as employment
    from {{ source('raw_data', 'hc_vacancies') }}
    where employment_type is not null

    union all

    select
        concat('hh_', v.id) as id,
        e.name as employment
    from {{ source('raw_data', 'hh_vacancies') }} as v
    join {{ source('raw_data', 'hh_employment') }} as e on e.id = v.employment_id
)

select
    id,
    employment
from data