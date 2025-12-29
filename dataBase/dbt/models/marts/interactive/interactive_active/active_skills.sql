{{ config(
    indexes=[
        {'columns': ['id', 'skill']}
    ]
)}}

select
    s.id as id,
    s.skill as skill
from {{ ref('stg_vacancies') }} as v
join {{ ref('stg_skills') }} as s on s.id = v.id
where v.closed_at is null