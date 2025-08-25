create materialized view internal.hh_salary as
    with salary as (
        select 
            v.id as id,
            case
                when v.salary_from is not null and v.salary_to is not null then 
                    (v.salary_from + v.salary_to) / (2 * c.rate)
                when v.salary_from is not null then 
                    v.salary_from / c.rate
                when v.salary_to is not null then 
                    v.salary_to / c.rate
            end as salary
        from hh_vacancies as v
        join currency as c on c.id = v.salary_currency_id
        where 
            (salary_from is not null or salary_to is not null)
            and v.salary_currency_id is not null
    ) select
        s.id,
        s.salary
    from salary as s
    where s.salary between 0 and 1000000;


create materialized view internal.gm_salary as
    with salary as (
        select 
            v.id as id,
            case
                when v.salary_from is not null and v.salary_to is not null then 
                    (v.salary_from + v.salary_to) / (2 * c.rate)
                when v.salary_from is not null then 
                    v.salary_from / c.rate
                when v.salary_to is not null then 
                    v.salary_to / c.rate
            end as salary
        from gm_vacancies as v
        join currency as c on c.id = v.salary_currency_id
        where 
            (salary_from is not null or salary_to is not null)
            and v.salary_currency_id is not null
    ) select
        s.id,
        s.salary
    from salary as s
    where s.salary between 0 and 1000000;


create materialized view internal.gj_salary as
    with salary as (
        select 
            v.id as id,
            case
                when v.salary_from is not null and v.salary_to is not null then 
                    (v.salary_from + v.salary_to) / (2 * c.rate)
                when v.salary_from is not null then 
                    v.salary_from / c.rate
                when v.salary_to is not null then 
                    v.salary_to / c.rate
            end as salary
        from gj_vacancies as v
        join currency as c on c.id = v.salary_currency_id
        where 
            (salary_from is not null or salary_to is not null)
            and v.salary_currency_id is not null
    ) select
        s.id,
        s.salary
    from salary as s
    where s.salary between 0 and 1000000;


create materialized view internal.fn_salary as
    with salary as (
        select 
            v.id as id,
            case
                when v.salary_from is not null and v.salary_to is not null then 
                    (v.salary_from + v.salary_to) / (2 * c.rate)
                when v.salary_from is not null then 
                    v.salary_from / c.rate
                when v.salary_to is not null then 
                    v.salary_to / c.rate
            end as salary
        from fn_vacancies as v
        join currency as c on c.id = v.salary_currency_id
        where 
            (salary_from is not null or salary_to is not null)
            and v.salary_currency_id is not null
    ) select
        s.id,
        s.salary
    from salary as s
    where s.salary between 0 and 1000000;