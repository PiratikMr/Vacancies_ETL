with data as (
    select
        concat('fn_', id) as id,
        salary_from,
        salary_to,
        salary_currency_id
    from {{ source('raw_data', 'fn_vacancies') }}

    union all

    select
        concat('gj_', id) as id,
        salary_from,
        salary_to,
        salary_currency_id
    from {{ source('raw_data', 'gj_vacancies') }}

    union all

    select
        concat('gm_', id) as id,
        salary_from,
        salary_to,
        salary_currency_id
    from {{ source('raw_data', 'gm_vacancies') }}

    union all

    select
        concat('hc_', id) as id,
        salary_from,
        salary_to,
        salary_currency_id
    from {{ source('raw_data', 'hc_vacancies') }}

    union all

    select
        concat('hh_', id) as id,
        salary_from,
        salary_to,
        salary_currency_id
    from {{ source('raw_data', 'hh_vacancies') }}

    union all

    select
        concat('az_', id) as id,
        salary_from,
        salary_to,
        salary_currency_id
    from {{ source('raw_data', 'az_vacancies') }}
), logic as (
    select
        v.id,
        case
            when v.salary_from is not null and v.salary_to is not null then 
                (v.salary_from + v.salary_to) / (2 * c.rate)
            when v.salary_from is not null then 
                v.salary_from / c.rate
            when v.salary_to is not null then 
                v.salary_to / c.rate
        end as salary,
        case
            when v.salary_from is not null and v.salary_to is not null then true
        else
            false
        end as has_range,
        c.id as currency
    from data as v
    join {{ source('raw_data', 'exchangerate_currency') }} as c on c.id = salary_currency_id
    where v.salary_from is not null or v.salary_to is not null
)

select 
    id,
    salary,
    has_range,
    currency
from logic
where salary between 1000 and 1000000