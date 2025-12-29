select
    count(*) as total,
    s.type as schedule,
    date_trunc('week', v.published_at) as week_start
from {{ ref('stg_vacancies') }} as v
join {{ ref('stg_schedules') }} as s on s.id = v.id
group by date_trunc('week', v.published_at), s.type