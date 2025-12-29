{{ config(
    indexes=[
        {'columns': ['id', 'schedule']}
    ]
)}}

select
    s.id as id,
    s.type as schedule
from {{ ref('stg_schedules') }} as s
join {{ ref('stg_vacancies') }} as v on v.id = s.id
where v.closed_at is null