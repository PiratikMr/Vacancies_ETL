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

/* experience by professionals roles

    role            |   avg_experience      |   percent
    ----------------+-----------------------+-----------
    data science    |   moreThan6           |   65
    data science    |   noExperience        |   35  
*/ 
drop materialized view exp_by_roles;
create materialized view exp_by_roles as
    with exp_count as (
        select
            role_id,
            count(*) as total
        from vacancies
        group by role_id
    ) select
            v.role_id,
            v.experience_id,
            round((count(*) * 100.0 / ec.total), 2) as percent
        from vacancies as v
        join exp_count as ec on ec.role_id = v.role_id
        group by 
            v.role_id,
            v.experience_id,
            ec.total
        order by v.role_id;
--//

--// schedule day by professionals roles
drop materialized view schedule_by_roles;
create materialized view schedule_by_roles as
    with schedule_count as (
        select
            role_id,
            count(*) as total
        from vacancies
        group by role_id
    ) select
            v.role_id,
            v.schedule_id,
            round((count(*) * 100.0 / sc.total), 2) as percent
        from vacancies as v
        join schedule_count as sc on sc.role_id = v.role_id
        group by 
            v.role_id,
            v.schedule_id,
            sc.total
        order by v.role_id;
--//

--// most popular experience
drop materialized view popular_experience;
create materialized view popular_experience as
    select
        v.experience_id as exp_id,
        count(*)
    from vacancies as v
    group by v.experience_id
    order by count desc
    limit 3;
--//

--// most popular role
drop materialized view popular_role;
create materialized view popular_role as
    select
        v.role_id,
        count(*)
    from vacancies as v
    group by v.role_id
    order by count desc
    limit 3;
--//

--// most popular schedule
drop materialized view popular_schedule;
create materialized view popular_schedule as
    select
        v.schedule_id,
        count(*)
    from vacancies as v
    group by v.schedule_id
    order by count desc
    limit 3;
--//

--// most popular currency
drop materialized view popular_currency;
create materialized view popular_currency as
    select
        v.currency_id,
        count(*)
    from vacancies as v
    group by v.currency_id
    order by count desc
    limit 3;
--//

--// most popular employment
drop materialized view popular_employment;
create materialized view popular_employment as
    select
        v.employment_id,
        count(*)
    from vacancies as v
    group by v.employment_id
    order by count desc
    limit 3;
--//