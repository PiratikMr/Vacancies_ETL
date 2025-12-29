{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    f.id as id,
    f.field as field,
    s.skill as skill
from {{ ref('active_fields') }} as f
join {{ ref('active_skills') }} as s on s.id = f.id