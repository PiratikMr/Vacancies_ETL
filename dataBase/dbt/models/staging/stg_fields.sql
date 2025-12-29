with data as (
    select
        concat('fn_',v.id) as id,
        f.name as field
    from {{ source('raw_data', 'fn_vacancies') }} as v
    left join {{ source('raw_data', 'fn_fields') }} as f on f.id = v.id

    union all

    select
        concat('gj_',v.id) as id,
        f.name as field
    from {{ source('raw_data', 'gj_vacancies') }} as v
    left join {{ source('raw_data', 'gj_fields') }} as f on f.id = v.id

    union all

    select
        concat('hc_',v.id) as id,
        f.name as field
    from {{ source('raw_data', 'hc_vacancies') }} as v
    left join {{ source('raw_data', 'hc_fields') }} as f on f.id = v.id

    union all

    select
        concat('hh_',v.id) as id,
        f.name as field
    from {{ source('raw_data', 'hh_vacancies') }} as v
    left join {{ source('raw_data', 'hh_professionalroles') }} as f on f.id = v.role_id
)

select
    id,
    field
from data
where field is not null