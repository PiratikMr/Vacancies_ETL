select
    date_trunc('week', v.published_at) as week_start,
    count(*) as total,
    percentile_cont(0.5) within group (order by s.salary) as salary_median,
    avg(s.salary) as salary_avg,

    avg(v.closed_at::date - v.published_at::date) as vacancy_live_avg,

    case 
        when count(*) = 0 then 
            0
        else
            count(case when s.salary is not null then 1 end) * 1.0 / count(*)
    end as has_salary,

    case 
        when count(*) = 0 then 
            0
        else
            count(case when s.has_range then 1 end) * 1.0 / count(*)
    end as has_range

from {{ ref('stg_vacancies') }} as v
left join {{ ref('stg_salaries') }} as s on s.id = v.id
group by date_trunc('week', v.published_at)