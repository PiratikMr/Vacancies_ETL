{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    v.id as id,
    v.employer as employer,
    v.salary as salary,
    g.grade as grade
from {{ ref('active_vacancies') }} as v
join {{ ref('active_grades') }} as g on g.id = v.id