create materialized view overview as
    select
            'HeadHunter' as platform,
            count(*) as vacs,
            count(case when v.is_active is true then 1 end) as active_vacs,
            round(avg(s.salary)) as salary
        from hh_vacancies as v
        left join internal.hh_salary as s on s.id = v.id

        union all

        select
            'GetMatch' as platform,
            count(*) as vacs,
            count(case when v.is_active is true then 1 end) as active_vacs,
            round(avg(s.salary)) as salary
        from gm_vacancies as v
        left join internal.gm_salary as s on s.id = v.id

        union all

        select
            'GeekJob' as platform,
            count(*) as vacs,
            count(case when v.is_active is true then 1 end) as active_vacs,
            round(avg(s.salary)) as salary
        from gj_vacancies as v
        left join internal.gj_salary as s on s.id = v.id

        union all

        select
            'Finder' as platform,
            count(*) as vacs,
            count(case when v.is_active is true then 1 end) as active_vacs,
            round(avg(s.salary)) as salary
        from fn_vacancies as v
        left join internal.fn_salary as s on s.id = v.id;


create materialized view overview_by_months as 
    with data as (
        select v.published_at, s.salary
        from hh_vacancies as v
        left join internal.hh_salary as s on s.id = v.id

        union all

        select v.published_at, s.salary
        from gm_vacancies as v
        left join internal.gm_salary as s on s.id = v.id

        union all

        select v.published_at, s.salary
        from gj_vacancies as v
        left join internal.gj_salary as s on s.id = v.id

        union all

        select v.published_at, s.salary
        from fn_vacancies as v
        left join internal.fn_salary as s on s.id = v.id
    ) select
        date_trunc('month', published_at),
        round(avg(salary)) as salary,
        count(*) as count
    from data
    group by date_trunc('month', published_at); 