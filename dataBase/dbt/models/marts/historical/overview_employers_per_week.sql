select
    date_trunc('week', v.published_at) as week_start,
    count(distinct e.employer) as total_employers,
    round(count(v.id)::numeric / count(distinct e.employer), 2) as vacancies_per_employer
from {{ ref('stg_vacancies') }} as v
join {{ ref('stg_employers') }} as e on e.id = v.id
group by date_trunc('week', v.published_at)