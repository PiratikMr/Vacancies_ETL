{{ config(
    indexes=[
      {'columns': ['id'], 'unique': True},
      {'columns': ['source']},
      {'columns': ['published_at']},
      {'columns': ['salary']},
      {'columns': ['has_range']},
      {'columns': ['currency']},
      {'columns': ['employer']},
      {'columns': ['employment']},
      {'columns': ['experience']}
    ]
) }}

select
    v.id as id,
    v.source as source,
    v.published_at as published_at,
    s.salary as salary,
    s.has_range as has_range,
    s.currency as currency,
    emplr.employer as employer,
    emplm.employment as employment,
    ex.experience as experience

from {{ ref('stg_vacancies') }} as v
left join {{ ref('stg_salaries') }} as s on s.id = v.id
left join {{ ref('stg_employers') }} as emplr on emplr.id = v.id
left join {{ ref('stg_employments') }} as emplm on emplm.id = v.id
left join {{ ref('stg_experience') }} as ex on ex.id = v.id
where v.closed_at is null