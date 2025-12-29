{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    g.id as id,
    g.grade as grade,
    v.salary as salary
from {{ ref('active_grades') }} as g
join {{ ref('active_vacancies') }} as v on v.id = g.id