{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    l.id as id,
    l.language as language,
    l.level as level,
    v.salary as salary
from {{ ref('active_languages') }} as l
join {{ ref('active_vacancies') }} as v on v.id = l.id