{{ config(
    indexes=[
        {'columns': ['id', 'grade']}
    ]
)}}

select
    g.id as id,
    g.grade as grade
from {{ ref('stg_grades') }} as g
join {{ ref('stg_vacancies') }} as v on v.id = g.id
where v.closed_at is null