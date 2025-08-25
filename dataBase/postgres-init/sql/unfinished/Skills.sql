create materialized view skills as
    with data as (
        select 
            name,
            avg(salary) as salary,
            count(*) as count
        from hh_skills as sk
        left join internal.hh_salary as s on s.id = sk.id
        group by name

        union all

        select 
            name,
            avg(salary) as salary,
            count(*) as count
        from gm_skills as sk
        left join internal.gm_salary as s on s.id = sk.id
        group by name

        union all

        select 
            name,
            avg(salary) as salary,
            count(*) as count
        from gj_skills as sk
        left join internal.gj_salary as s on s.id = sk.id
        group by name
    ) select
        name,
        round(avg(salary)) as salary,
        sum(count) as count
    from data
    group by name;