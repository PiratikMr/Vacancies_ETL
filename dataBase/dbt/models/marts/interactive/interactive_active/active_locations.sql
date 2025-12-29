{{ config(
    indexes=[
        {'columns': ['id', 'region', 'country']}
    ]
)}}

select
    l.id as id,
    l.region as region,
    l.country as country,
    l.lat as lat,
    l.lng as lng
from {{ ref('stg_locations') }} as l
join {{ ref('stg_vacancies') }} as v on v.id = l.id
where v.closed_at is null