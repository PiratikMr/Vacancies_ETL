{{ config(
    indexes=[
        {'columns': ['id']}
    ]
)}}

select
    a.id as id,
    a.skill as skill1,
    b.skill as skill2
from {{ ref('active_skills') }} as a
join {{ ref('active_skills') }} as b on a.id = b.id and a.skill < b.skill