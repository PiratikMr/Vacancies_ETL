with data as (
    select
        concat('fn_', id) as id,
        case
            when lower(title) ~ '.*(intern|стаж).*' then 'Стажер'
            when lower(title) ~ '.*(jun|джун).*' then 'Джуниор'
            when lower(title) ~ '.*(midd|мидд).*' then 'Миддл'
            when lower(title) ~ '.*(sen|сеньор).*' then 'Сеньор'
        end as grade
    from {{ source('raw_data', 'fn_vacancies') }}

    union all

    select
        concat('gj_', v.id) as id,
        g.name as grade
    from {{ source('raw_data', 'gj_vacancies') }} as v
    join {{ source('raw_data', 'gj_grades') }} as g on g.id = v.id

    union all

    select
        concat('gm_', id) as id,
        case
            when level = 'Senior' then 'Сеньор'
            when level = 'Lead' then 'Тимлид/Руководитель группы'
            when level = 'Middle' then 'Миддл'
            when level = 'Middle-to-Senior' then 'Миддл'
            when level = 'C-level' then 'Руководитель отдела/подразделения'
            when level = 'Junior' then 'Джуниор'
        end as grade
    from {{ source('raw_data', 'gm_vacancies') }}

    union all

    select
        concat('hc_', id) as id,
        case
            when grade = 'Младший (Junior)' then 'Джуниор'
            when grade = 'Ведущий (Lead)' then 'Тимлид/Руководитель группы'
            when grade = 'Средний (Middle)' then 'Миддл'
            when grade = 'Стажёр (Intern)' then 'Стажер'
            when grade = 'Старший (Senior)' then 'Сеньор'
        end as grade
    from {{ source('raw_data', 'hc_vacancies') }}

    union all

    select
        concat('hh_', id) as id,
        case
            when lower(title) ~ '.*(intern|стаж).*' then 'Стажер'
            when lower(title) ~ '.*(jun|джун).*' then 'Джуниор'
            when lower(title) ~ '.*(midd|мидд).*' then 'Миддл'
            when lower(title) ~ '.*(sen|сеньор).*' then 'Сеньор'
        end as grade
    from {{ source('raw_data', 'hh_vacancies') }}
)

select
    id,
    grade
from data
where grade is not null