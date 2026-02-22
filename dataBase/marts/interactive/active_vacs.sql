create schema interactive;

create materialized view salary as
    select
        f.vacancy_id,
        case
            when f.salary_from is not null and f.salary_to is not null then 
                (f.salary_from + f.salary_to) / (2 * c.rate)
            when f.salary_from is not null then 
                f.salary_from / c.rate
            when f.salary_to is not null then 
                f.salary_to / c.rate
        end as salary,
        case
            when f.salary_from is not null and f.salary_to is not null then true
        else
            false
        end as has_range
    from fact_vacancy as f
    join dim_currency as c on c.currency_id = f.currency_id
    where f.salary_from is not null or f.salary_to is not null;