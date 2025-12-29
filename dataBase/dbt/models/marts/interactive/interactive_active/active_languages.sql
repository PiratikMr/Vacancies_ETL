{{ config(
    indexes=[
        {'columns': ['id', 'language', 'level']}
    ]
)}}

select
    l.id as id,
    l.language as language,
    l.level as level
from {{ ref('stg_languages') }} as l
join {{ ref('stg_vacancies') }} as v on v.id = l.id
where v.closed_at is null