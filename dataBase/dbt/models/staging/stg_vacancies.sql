with data as (
    select
        concat('fn_', id) as id,
        closed_at,
        'Finder' as source,
        published_at
    from {{ source('raw_data', 'fn_vacancies') }}

    union all

    select
        concat('gj_', id) as id,
        closed_at,
        'GeekJob' as source,
        published_at
    from {{ source('raw_data', 'gj_vacancies') }}

    union all

    select
        concat('gm_', id) as id,
        closed_at,
        'GetMatch' as source,
        published_at
    from {{ source('raw_data', 'gm_vacancies') }}s

    union all

    select
        concat('hc_', id) as id,
        closed_at,
        'HabrCareer' as source,
        published_at
    from {{ source('raw_data', 'hc_vacancies') }}

    union all

    select
        concat('hh_', id) as id,
        closed_at,
        'HeadHunter' as source,
        published_at
    from {{ source('raw_data', 'hh_vacancies') }}
) 

select
    id,
    closed_at,
    source,
    published_at
from data