select
    count(*) as total,
    percentile_cont(0.5) within group (order by s.salary) as salary_median,
    round(case when count(*) = 0 then 0 else
        count(case when s.salary is not null then 1 end)*1.0 / count(*)
        end, 2) as has_salary,
    round(case when count(*) = 0 then 0 else
        count(case when s.has_range then 1 end)*1.0 / count(*)
        end, 2) as has_range
from {{ ref('stg_vacancies') }} as v
left join {{ ref('stg_salaries') }} as s on s.id = v.id