{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    v.id as id,
    v.salary as salary,
    l.language as language
from {{ ref('active_vacancies') }} as v
left join {{ ref('active_languages') }} as l on l.id = v.id