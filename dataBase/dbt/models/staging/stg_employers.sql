with data as (
    select
        concat('fn_', id) as id,
        employer
    from {{ source('raw_data', 'fn_vacancies') }}

    union all

    select
        concat('gj_', id) as id,
        employer
    from {{ source('raw_data', 'gj_vacancies') }}

    union all

    select
        concat('gm_', id) as id,
        employer
    from {{ source('raw_data', 'gm_vacancies') }}

    union all

    select
        concat('hc_', id) as id,
        employer
    from {{ source('raw_data', 'hc_vacancies') }}

    union all

    select
        concat('hh_', v.id) as id,
        e.name as employer
    from {{ source('raw_data', 'hh_vacancies') }} as v
    join {{ source('raw_data', 'hh_employers') }} as e on e.id = v.employer_id

    union all

    select
        concat('az_', v.id) as id,
        v.employer as employer
    from {{ source('raw_data', 'az_vacancies') }} as v
) 

select
    id,
    employer
from data
where employer is not null