-- KPI (Всего вакансий, Вакансии с зарплатой, Вакансии с вилкой, Медиана зарплаты)
create materialized view historical.overview_kpi as
    select
        count(*) as total,
        percentile_cont(0.5) within group (order by s.salary) as salary_median,
        round(case when count(*) = 0 then 0 else
            count(case when s.salary is not null then 1 end)*1.0 / count(*)
            end, 2) as has_salary,
        round(case when count(*) = 0 then 0 else
            count(case when s.has_range then 1 end)*1.0 / count(*)
            end, 2) as has_range
    from core.vacancies as v
    left join core.salaries as s on s.id = v.id;


create materialized view historical.overview_vacancies_salaries_for_sources_per_week as
    select
        count(*) total,
        v.source as source,
        percentile_cont(0.5) within group (order by s.salary) as salary_median,
        date_trunc('week', v.published_at) as week_start
    from core.vacancies as v
    left join core.salaries as s on s.id = v.id
    group by date_trunc('week', v.published_at), v.source;


create materialized view historical.overview_employers_per_week as
    select
        date_trunc('week', v.published_at) as week_start,
        count(distinct e.employer) as total_employers,
        round(count(v.id)::numeric / count(distinct e.employer), 2) as vacancies_per_employer
    from core.vacancies as v
    join core.employers as e on e.id = v.id
    group by date_trunc('week', v.published_at);


create materialized view historical.overview_schedules_per_week as
    select
        count(*) as total,
        s.type as schedule,
        date_trunc('week', v.published_at) as week_start
    from core.vacancies as v
    join core.schedules as s on s.id = v.id
    group by date_trunc('week', v.published_at), s.type;