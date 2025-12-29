with data as (
    select
        concat('gj_', v.id) as id,
        s.name as skill
    from {{ source('raw_data', 'gj_vacancies') }} as v
    join {{ source('raw_data', 'gj_skills') }} as s on s.id = v.id

    union all

    select
        concat('gm_', v.id) as id,
        s.name as skill
    from {{ source('raw_data', 'gm_vacancies') }} as v
    join {{ source('raw_data', 'gm_skills') }} as s on s.id = v.id

    union all

    select
        concat('hc_', v.id) as id,
        s.name as skill
    from {{ source('raw_data', 'hc_vacancies') }} as v
    join {{ source('raw_data', 'hc_skills') }} as s on s.id = v.id

    union all

    select
        concat('hh_', v.id) as id,
        s.name as skill
    from {{ source('raw_data', 'hh_vacancies') }} as v
    join {{ source('raw_data', 'hh_skills') }} as s on s.id = v.id
)

select
    id,
    skill
from data
where skill is not null