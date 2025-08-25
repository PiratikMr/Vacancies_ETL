create materialized view external.skills_salary as
    with data as (
        select 
            name,
            count(*),
            avg(s.salary) as salary
        from hh_skills as sk
        left join internal.hh_salary as s on s.id = sk.id
        group by name

        union all

        select 
            name,
            count(*),
            avg(s.salary) as salary
        from gj_skills as sk
        left join internal.gj_salary as s on s.id = sk.id
        group by name

        union all

        select 
            name,
            count(*),
            avg(s.salary) as salary
        from gm_skills as sk
        left join internal.gm_salary as s on s.id = sk.id
        group by name
    )
    select name, sum(count), round(avg(salary)) from data
    group by name;