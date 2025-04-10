--// WITH VIEWS

    --// mediane, average
    drop materialized view avg_sal;
    create materialized view avg_sal as
        select 
            v.id,
            (v.salary_from + v.salary_to) / (2 * c.rate) as salary
        from vacancies as v
        join currency as c on c.id = v.currency_id
        where (v.salary_from is not null and v.salary_to is not null);
    --//   
--//


--// FINAL VIEWS

--//  sal_by_roles
drop materialized view if exists sal_by_roles;
create materialized view sal_by_roles as
    with mv as (
        select
            v.role_id,
            round(cast(percentile_cont(0.5) within group (order by avs.salary) as numeric), 2) as med,
            round(cast(avg(avs.salary) as numeric), 2) as avg
        from avg_sal as avs
        join vacancies as v on v.id = avs.id
        group by v.role_id
    )
    select 
        r.name as role_name,
        mv.med as mediane,
        mv.avg as average        
    from mv
    join roles as r on r.id = mv.role_id
    order by r.name;
--//

--// sal_by_years
drop materialized view if exists sal_by_years;
create materialized view sal_by_years as
    with mv as (
        select
            extract(year from v.publish_date) as year,
            v.role_id,
            round(cast(percentile_cont(0.5) within group (order by avs.salary) as numeric), 2) as med,
            round(cast(avg(avs.salary) as numeric), 2) as avg
        from avg_sal as avs
        join vacancies as v on v.id = avs.id
        group by extract(year from v.publish_date), v.role_id
    )
    select
        year,
        r.name as role_name,
        mv.med as mediane,
        mv.avg as average
    from mv
    join roles as r on r.id = mv.role_id
    order by year desc;
--//

--// sal_by_countries
drop materialized view if exists sal_by_countries;
create materialized view sal_by_countries as
    with mv as (
        select
            v.country_area_id,
            round(cast(percentile_cont(0.5) within group (order by avs.salary) as numeric), 2) as med,
            round(cast(avg(avs.salary) as numeric), 2) as avg
        from avg_sal as avs
        join vacancies as v on v.id = avs.id
        group by v.country_area_id
    )
    select
        a.name as country,
        mv.med as mediane,
        mv.avg as average
    from mv
    join areas as a on a.id = mv.country_area_id
    order by a.name;
--//

--// vac_by_countries
drop materialized view if exists vac_by_countries;
create materialized view vac_by_countries as
    with total as (
        select count(*) as total from vacancies
    ),
    cc as (
        select 
            v.country_area_id as country,
            count(*) as cnt
        from vacancies as v
        group by v.country_area_id
    )
    select 
        a.name as country,
        round(cc.cnt * 100.0 / t.total, 2) as percent
    from cc
    join areas as a on a.id = cc.country
    cross join total as t
    order by percent;
--//


/* experience by professionals roles

    role            |   avg_experience      |   percent
    ----------------+-----------------------+-----------
    data science    |   moreThan6           |   65
    data science    |   noExperience        |   35  
*/ 
drop materialized view if exists exp_by_roles;
create materialized view exp_by_roles as
    with total as (
        select
            v.role_id,
            v.experience_id,
            count(*) as exp_total,
            sum(count(*)) over (partition by v.role_id) as total
        from vacancies as v
        group by v.role_id, v.experience_id
    ) 
    select
        r.name as role_name,
        exp.name as experience_name,
        round(t.exp_total * 100.0 / t.total, 2) as percent
    from total as t
    join roles as r on r.id = t.role_id
    join experience as exp on exp.id = t.experience_id
    order by r.name;
--//

--// schedule day by professionals roles
drop materialized view if exists schedule_by_roles;
create materialized view schedule_by_roles as
    with total as (
        select
            v.role_id,
            v.schedule_id,
            count(*) as sch_total,
            sum(count(*)) over (partition by v.role_id) as total
        from vacancies as v
        group by v.role_id, v.schedule_id
    ) 
    select
            r.name as role_name,
            sch.name as schedule_name,
            round(t.sch_total * 100.0 / t.total, 2) as percent
        from total as t
        join schedule as sch on sch.id = t.schedule_id
        join roles as r on r.id = t.role_id
        order by r.name;
--//

--// most popular experiences
drop materialized view if exists pop_experiences;
create materialized view pop_experiences as
    with total as (
        select
            v.experience_id,
            count(*) as exp_total,
            sum(count(*)) over () as total
        from vacancies as v
        group by v.experience_id
    )
    select
        e.name as experience_name,
        round(t.exp_total * 100.0 / t.total, 2) as percent
    from total as t
    join experience as e on e.id = t.experience_id
    order by percent desc;
--//

--// most popular roles
drop materialized view if exists pop_roles;
create materialized view pop_roles as
    with total as (
        select
            v.role_id,
            count(*) as role_total,
            sum(count(*)) over () as total
        from vacancies as v
        group by v.role_id
    )
    select
        r.name as role_name,
        round(t.role_total * 100.0 / t.total, 2) as percent
    from total as t
    join roles as r on r.id = t.role_id
    order by percent desc;
--//

--// most popular schedules
drop materialized view if exists pop_schedules;
create materialized view pop_schedules as
    with total as (
        select
            v.schedule_id,
            count(*) as sch_total,
            sum(count(*)) over () as total
        from vacancies as v
        group by v.schedule_id
    )
    select
        sc.name as schedule_name,
        round(t.sch_total * 100.0 / t.total, 2) as percent
    from total as t
    join schedule as sc on sc.id = t.schedule_id
    order by percent desc;
--//

--// most popular currencies
drop materialized view if exists pop_currencies;
create materialized view pop_currencies as
    with total as (
        select
            v.currency_id,
            count(*) as curr_total,
            sum(count(*)) over () as total
        from vacancies as v
        where currency_id is not null
        group by v.currency_id
    )
    select
        c.name as currency_name,
        round(t.curr_total * 100.0 / t.total, 2) as percent
    from total as t
    join currency as c on c.id = t.currency_id
    order by percent desc;
--//

--// most popular employments
drop materialized view if exists pop_employments;
create materialized view pop_employments as
    with total as (
        select
            v.employment_id,
            count(*) as empl_total,
            sum(count(*)) over () as total
        from vacancies as v
        group by v.employment_id
    )
    select
        e.name as employment_name,
        round(t.empl_total * 100.0 / t.total, 2) as percent
    from total as t
    join employment as e on e.id = t.employment_id
    order by percent desc;
--//