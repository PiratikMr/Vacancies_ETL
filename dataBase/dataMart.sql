--//  sal_by_roles
drop materialized view sal_by_role;
create materialized view sal_by_role as
    select 
        r.id as role_id,
        r.name as role_name,
        round(max(GREATEST(v.salary_to, v.salary_from) / c.rate)) as max,
        round(avg((v.salary_to + v.salary_from) / (2 * c.rate))) as avg
    from 
        vacancies as v
    join roles as r on r.id = v.role_id
    join currency as c on c.id = v.currency_id
    group by 
        r.id,
        r.name
    order by
        avg;
--//


--// sal_by_years
drop materialized view sal_by_year;
create materialized view sal_by_year as
    with yearly_salaries as (
        select
            extract(year from v.publish_date) as year,
            role_id,
            round(max(greatest(v.salary_from, v.salary_to) / c.rate)) as max_salary,
            round(avg((v.salary_from + v.salary_to) / (2 * c.rate))) as avg_salary
        from
            vacancies as v
        join currency as c on c.id = v.currency_id
        group by
            extract(year from publish_date),
            role_id
    ),
    ranked_roles as (
        select
            year,
            role_id,
            max_salary,
            avg_salary,
            rank() over (partition by year order by avg_salary desc) as rank
        from
            yearly_salaries
    )
    select
        year,
        r.name as role_name,
        max_salary,
        avg_salary
    from
        ranked_roles as rr
    join roles as r on r.id = rr.role_id
    where
        rank <= 3
    order by
        year desc,
        rank;
--//


--// sal_by_country
drop materialized view sal_by_country;
create materialized view sal_by_country as
    select
        a.name as country,
        round(max(greatest(v.salary_to, v.salary_from) / c.rate)) as max,
        round(avg((v.salary_to + v.salary_from) / (2 * c.rate))) as avg
    from
        vacancies as v
    join currency as c on c.id = v.currency_id
    join areas as a on a.id = v.country_area_id
    group by
        a.name 
    order by
        avg;
--//


--// vac_count_by_country
drop materialized view vac_count_by_country;
create materialized view vac_count_by_country as
    select 
        a.name as country,
        count(*) as count
    from vacancies as v
    join areas as a on a.id = v.country_area_id
    group by
        a.name
    order by
        count;
--//


