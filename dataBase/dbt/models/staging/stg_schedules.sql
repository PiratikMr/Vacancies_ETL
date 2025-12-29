with data as (
    select
        concat('fn_', id) as id,
        'Удаленная работа' as type
    from {{ source('raw_data', 'fn_vacancies') }}
    where distant_work is true

    union all

    select
        concat('gj_', v.id) as id,
        jf.name as type
    from {{ source('raw_data', 'gj_vacancies') }} as v
    join gj_jobformats as jf on jf.id = v.id

    union all

    select
        concat('gm_', id) as id,
        'Удаленная работа' as type
    from {{ source('raw_data', 'gm_vacancies') }}
    where remote_options is not null

    union all

    select
        concat('gm_', id) as id,
        'Работа в офисе' as type
    from {{ source('raw_data', 'gm_vacancies') }}
    where office_options is not null

    union all

    select
        concat('hc_', id) as id,
        'Удаленная работа' as type
    from {{ source('raw_data', 'hc_vacancies') }}
    where remote_work is true

    union all

    select
        concat('hh_', v.id) as id,
        sc.name as type
    from {{ source('raw_data', 'hh_vacancies') }} as v
    join {{ source('raw_data', 'hh_schedule') }} as sc on sc.id = v.schedule_id
)

select
    id,
    type
from data
where type is not null