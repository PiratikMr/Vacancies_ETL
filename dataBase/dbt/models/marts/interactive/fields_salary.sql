{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    f.id as id,
    f.field as field,
    v.salary as salary
from {{ ref('active_fields') }} as f
join {{ ref('active_vacancies') }} as v on v.id = f.id