{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    v.id as id,
    v.salary as salary,
    s.skill as skill
from {{ ref('active_vacancies') }} as v
join {{ ref('active_skills') }} as s on s.id = v.id