create materialized view external.countdata as
    with counts as (

        select 
            'HeadHunter' as site,
            count(*) as count,
            count(case when v.salary_from is not null and v.salary_to is not null then 1 end) as with_range,
            count(s.*) as with_salary,
            count(case when is_active is true then 1 end) as active
        from hh_vacancies as v
        left join internal.hh_salary as s on s.id = v.id

        union all

        select 
            'GeekJob' as site,
            count(*) as count,
            count(case when v.salary_from is not null and v.salary_to is not null then 1 end) as with_range,
            count(s.*) as with_salary,
            count(case when is_active is true then 1 end) as active
        from gj_vacancies as v
        left join internal.gj_salary as s on s.id = v.id

        union all

        select 
            'GetMatch' as site,
            count(*) as count,
            count(case when v.salary_from is not null and v.salary_to is not null then 1 end) as with_range,
            count(s.*) as with_salary,
            count(case when is_active is true then 1 end) as active
        from gm_vacancies as v
        left join internal.gm_salary as s on s.id = v.id
    )
    select 
        site,
        count,
        with_range, 
        with_salary,
        active
    from counts
    union all
    select 
        'Все' as site,
        sum(count) as count,
        sum(with_range) as with_range,
        sum(with_salary) as with_salary,
        sum(active) as active
    from counts;


-- create materialized view external.countdata_by_days as
--     with data as (
--         select published_at from hh_vacancies
--         union all
--         select published_at from gj_vacancies
--         union all
--         select published_at from gm_vacancies
--     )
--     select date_trunc('day', published_at), count(*) from
--     data group by date_trunc('day', published_at);