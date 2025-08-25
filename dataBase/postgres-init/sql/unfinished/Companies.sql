create materialized view companies as
    with data as (
        select
            v.employer,
            s.salary
        from fn_vacancies as v
        left join internal.fn_salary as s on s.id = v.id

        union all

        select
            v.employer,
            s.salary
        from gj_vacancies as v
        left join internal.gj_salary as s on s.id = v.id

        union all

        select
            v.employer,
            s.salary
        from gm_vacancies as v
        left join internal.gm_salary as s on s.id = v.id

        union all

        select
            e.name as employer,
            s.salary
        from hh_vacancies as v
        join hh_employers as e on e.id = v.employer_id
        left join internal.hh_salary as s on s.id = v.id
    ) select
        employer,
        round(avg(salary)) as salary,
        count(*)
    from data
    group by employer;