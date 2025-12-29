{{ config(
    indexes=[
        {'columns': ['id', 'field']}
    ]    
)}}

select
    f.id as id,
    f.field as field
from {{ ref('stg_fields') }} as f
join {{ ref('stg_vacancies') }} as v on v.id = f.id
where v.closed_at is null