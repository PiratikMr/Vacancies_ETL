{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    s.id as id,
    s.schedule as schedule,
    f.field as field
from {{ ref('active_schedules') }} as s
join {{ ref('active_fields') }} as f on f.id = s.id