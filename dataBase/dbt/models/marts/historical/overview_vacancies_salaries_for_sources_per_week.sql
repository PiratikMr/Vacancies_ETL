select
    count(*) total,
    v.source as source,
    percentile_cont(0.5) within group (order by s.salary) as salary_median,
    date_trunc('week', v.published_at) as week_start
from {{ ref('stg_vacancies') }} as v
left join {{ ref('stg_salaries') }} as s on s.id = v.id
group by date_trunc('week', v.published_at), v.source