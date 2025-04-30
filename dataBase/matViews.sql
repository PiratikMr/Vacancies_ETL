--// BASE
    --0.1 Transformed hh salaries
        drop materialized view if exists avg_sal_hh;
        create materialized view avg_sal_hh as
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
                join currency as c on c.id = v.currency_id
                where 
                    (salary_from is not null or salary_to is not null)
                    and v.currency_id is not null
            ) select
                s.id,
                s.salary
            from salary as s
            where s.salary between 10000 and 1000000;

    --0.2 Transformed gj salaries
        drop materialized view if exists avg_sal_gj;
        create materialized view avg_sal_gj as
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
                join currency as c on c.id = v.currency_id
                where 
                    (salary_from is not null or salary_to is not null)
                    and v.currency_id is not null
            ) select
                s.id,
                s.salary
            from salary as s
            where s.salary between 10000 and 1000000;

    --0.3 Transformed gm salaries
        drop materialized view if exists avg_sal_gm;
        create materialized view avg_sal_gm as
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
                join currency as c on c.id = v.currency_id
                where 
                    (salary_from is not null or salary_to is not null)
                    and v.currency_id is not null
            ) select
                s.id,
                s.salary
            from salary as s
            where s.salary between 10000 and 1000000;
--//


--// Materialized views

    --1. Number of vacancies
        drop materialized view if exists vacs_count;
        create materialized view vacs_count as
            with counts as (
                select
                    count(*) as total,
                    count(case when salary_from is not null and salary_to is not null then 1 end) as range_count,
                    count(case when salary_from is not null or salary_to is not null then 1 end) as salary_count,
                    count(case when currency_id in ('USD', 'EUR') then 1 end) ed_count
                from (
                    select cast(id as text), salary_from, salary_to, currency_id from hh_vacancies
                    union all
                    select cast(id as text), salary_from, salary_to, currency_id from gm_vacancies
                    union all
                    select id, salary_from, salary_to, currency_id from gj_vacancies
                )
            ), skills as (
                select
                    (select count(distinct id) from hh_skills) + 
                    (select count(distinct id) from gj_skills) + 
                    (select count(distinct id) from gm_skills) as res
            ), companies as (
                select
                    count(distinct employer) as res
                from (
                    select name as employer from hh_employers
                    union all
                    select distinct employer from gm_vacancies
                    union all
                    select distinct employer from gj_vacancies
                )
            ), foreigns as (
                select
                    (
                        select 
                            count(*) 
                        from hh_vacancies
                        where country_area_id != 113
                    ) + 
                    (
                        select count(distinct id)
                        from gm_locations 
                        where lower(country) not like '%росс%' or lower(country) not like '%рф%'
                    ) + 
                    (
                        select count(distinct id)
                        from gj_locations
                        where 
                            lower(name) not like '%роcс%' or
                            lower(name) not like '%рф%' or
                            lower(name) not like '%rus%'
                    ) as res
            ) select 
                c.total as total, 
                round(1.0 * c.range_count / c.total, 3) as rng, 
                round(1.0 * c.ed_count / c.total, 3) as ed, 
                round(1.0 * c.salary_count / c.total, 3) as salary,
                skills.res as skill_count,
                comp.res as company_count,
                round(1.0 * c.total / comp.res ,1) as vacs_on_comp,
                round(1.0 * f.res / c.total, 3) as foreigns

                from counts as c
                cross join skills
                cross join companies as comp
                cross join foreigns as f;

    --2. Average, mediane, min, max salary in rubles
        drop materialized view if exists avg_med_sal;
        create materialized view avg_med_sal as
            with comm_sal as (
                select salary
                from (
                    select salary from avg_sal_hh
                    union all
                    select salary from avg_sal_gj
                    union all
                    select salary from avg_sal_gm
                ) as salary
            ) select 
                round(avg(c.salary)) as average, 
                round(cast(percentile_cont(0.5) within group (order by c.salary) as numeric)) as mediane,
                round(min(c.salary)) as min,
                round(max(c.salary)) as max
            from comm_sal as c;

    --3. Top companies by salary
        drop materialized view if exists top_companies_by_salary;
        create materialized view top_companies_by_salary as
            with companies_hh as (
                select 
                    e.name as name,
                    count(*) as count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from avg_sal_hh as s
                join hh_vacancies as h on h.id = s.id
                join hh_employers as e on e.id = h.employer_id
                where h.employer_id is not null
                group by e.name
            ), companies_gj as (
                select 
                    g.employer as name,
                    count(*) count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from avg_sal_gj as s
                join gj_vacancies as g on g.id = s.id
                where g.employer is not null
                group by g.employer
            ), companies_gm as (
                select 
                    g.employer as name,
                    count(*) count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from avg_sal_gm as s
                join gm_vacancies as g on g.id = s.id
                where g.employer is not null
                group by g.employer
            ) select 
                c.name,
                sum(c.count) as count,
                round(cast(percentile_cont(0.5) within group (order by c.salary) as numeric)) as salary
            from (
                select * from companies_hh 
                union all
                select * from companies_gj
                union all
                select * from companies_gm 
            ) as c
            where count > 5
            group by c.name
            order by salary desc
            limit 100;
            
    --4. Top skills, mediane salary for each
        drop materialized view if exists top_skills;
        create materialized view top_skills as
            with hh as (
                select
                    sk.name,
                    count(*) as count,
                    round(cast(percentile_cont(0.5) within group (order by sa.salary) as numeric)) as salary
                from hh_vacancies as v
                join hh_skills as sk on v.id = sk.id
                left join avg_sal_hh as sa on sa.id = v.id
                where sk.name is not null
                group by sk.name
            ), gj as (
                select
                    sk.name,
                    count(*) as count,
                    round(cast(percentile_cont(0.5) within group (order by sa.salary) as numeric)) as salary
                from gj_vacancies as v
                join gj_skills as sk on v.id = sk.id
                left join avg_sal_gj as sa on sa.id = v.id
                where sk.name is not null
                group by sk.name
            ), gm as (
                select
                    sk.name,
                    count(*) as count,
                    round(cast(percentile_cont(0.5) within group (order by sa.salary) as numeric)) as salary
                from gm_vacancies as v
                join gm_skills as sk on v.id = sk.id
                left join avg_sal_gm as sa on sa.id = v.id
                where sk.name is not null
                group by sk.name
            ) select
                name,
                sum(count) as count,
                round(cast(percentile_cont(0.5) within group (order by salary) as numeric)) as salary
            from (
                select * from hh 
                union all
                select * from gj
                union all
                select * from gm 
            )
            group by name
            order by count desc
            limit 100;

    --5. English level by salary
        drop materialized view if exists english_level;
        create materialized view english_level as
            select
                case
                    when v.english_lvl is null then
                        '-'
                    else v.english_lvl
                end as name,
                count(*) as count,
                round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
            from gm_vacancies as v
            join avg_sal_gm as s on s.id = v.id
            group by v.english_lvl
            order by salary desc;

    --6. Top fields/specialisation by salary
        drop materialized view if exists top_fields;
        create materialized view top_fields as
            with hh as (
                select
                    r.name as name,
                    count(*) as count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from hh_vacancies as v
                join avg_sal_hh as s on s.id = v.id
                join hh_roles as r on r.id = v.role_id
                group by r.name
            ), gj as (
                select 
                    f.name as name,
                    count(*) as count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from gj_fields as f
                join avg_sal_gj as s on s.id = f.id
                group by f.name
            ) select
                name,
                sum(count) as count,
                round(cast(percentile_cont(0.5) within group (order by salary) as numeric)) as salary
            from (
                select * from hh 
                union all
                select * from gj 
            )
            group by name
            order by count desc
            limit 100;

    --7. Level by salary
        drop materialized view if exists levels_salary;
        create materialized view levels_salary as
            select
                l.name,
                count(*) as count,
                round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
            from gj_level as l
            join avg_sal_gj as s on s.id = l.id
            group by l.name
            order by count desc;

    --8. Top experience by salary
        drop materialized view if exists top_experience;
        create materialized view top_experience as
            with hh as (
                select
                    e.name,
                    count(*) as count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from hh_vacancies as v
                join avg_sal_hh as s on s.id = v.id
                join hh_experience as e on e.id = v.experience_id
                group by e.name
            ), gj as (
                select
                    case 
                        when v.experience like '%От%3%до%' then 'От 3 до 6 лет'
                        when v.experience like '%Более%5%' then 'Более 6 лет'
                        else v.experience
                    end as experience,
                    count(*) as count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from gj_vacancies as v
                join avg_sal_gj as s on s.id = v.id
                group by v.experience
            ) select
                name,
                sum(count) as count,
                round(cast(percentile_cont(0.5) within group (order by salary) as numeric)) as salary
            from (
                select * from hh
                union all
                select * from gj
            )
            group by name
            order by count desc;
--//