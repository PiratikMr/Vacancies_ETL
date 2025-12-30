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
    from {{ source('raw_data', 'gm_vacancies') }}

    union all

    select
        concat('hh_', v.id) as id,
        l.name as language,
        l.level as level
    from {{ source('raw_data', 'hh_vacancies') }} as v
    join {{ source('raw_data', 'hh_languages') }} as l on l.id = v.id

    union all

    select
        concat('az_', id) as id,
        'Английский' as language,
        'C1 — Продвинутый' as level
    from {{ source('raw_data', 'az_vacancies') }}
)

select
    id,
    language,
    level
from data