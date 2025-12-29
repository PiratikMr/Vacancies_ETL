{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    l.id as id,
    l.region as region,
    l.country as country,
    l.lat as lat,
    l.lng as lng,
    v.salary as salary
from {{ ref('active_locations') }} as l
join {{ ref('active_vacancies') }} as v on v.id = l.id