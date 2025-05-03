--// BASE
    --0.1 Transformed hh salaries
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

    --! add fields count
    --1. Number of vacancies
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
                case when c.total = 0 then 0 else round(1.0 * c.range_count / c.total, 3) end as rng, 
                case when c.total = 0 then 0 else round(1.0 * c.ed_count / c.total, 3) end as ed, 
                case when c.total = 0 then 0 else round(1.0 * c.salary_count / c.total, 3) end as salary,
                skills.res as skill_count,
                comp.res as company_count,
                case when comp.res = 0 then 0 else round(1.0 * c.total / comp.res ,1) end as vacs_on_comp,
                case when c.total = 0 then 0 else round(1.0 * f.res / c.total, 3) end as foreigns

                from counts as c
                cross join skills
                cross join companies as comp
                cross join foreigns as f;

    --2. Average, mediane, min, max salary in rubles
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
        create materialized view top_companies as
            with hh as (
                select 
                    e.name as name,
                    count(*) as count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from hh_vacancies as v
                join hh_employers as e on e.id = v.employer_id
                left join avg_sal_hh as s on s.id = v.id
                group by e.name
            ), gj as (
                select 
                    v.employer as name,
                    count(*) count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from gj_vacancies as v
                left join avg_sal_gj as s on s.id = v.id
                where v.employer is not null
                group by v.employer
            ), gm as (
                select 
                    v.employer as name,
                    count(*) count,
                    round(cast(percentile_cont(0.5) within group (order by s.salary) as numeric)) as salary
                from gm_vacancies as v
                left join avg_sal_gm as s on s.id = v.id
                where v.employer is not null
                group by v.employer
            ) select 
                c.name,
                sum(c.count) as count,
                round(cast(percentile_cont(0.5) within group (order by c.salary) as numeric)) as salary
            from (
                select * from hh 
                union all
                select * from gj
                union all
                select * from gm 
            ) as c
            where count > 5
            group by c.name
            order by count desc
            limit 100;
            
    --4. Top skills, mediane salary for each
        create materialized view top_skills as
            with hh as (
                select
                    sk.name,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from hh_skills as sk
                left join avg_sal_hh as s on s.id = sk.id
                where sk.name is not null
                group by sk.name
            ), gj as (
                select
                    sk.name,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from gj_skills as sk
                left join avg_sal_gj as s on s.id = sk.id
                where sk.name is not null
                group by sk.name
            ), gm as (
                select
                    sk.name,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from gm_skills as sk
                left join avg_sal_gm as s on s.id = sk.id
                where sk.name is not null
                group by sk.name
            ) select
                case when name is null then 'Без навыка' else name end as name,
                sum(count) as count,
                round(avg(salary)) as salary
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
        create materialized view english_level as
            select
                case
                    when v.english_lvl is null then
                        'Не требуется'
                    else v.english_lvl
                end as name,
                count(*) as count,
                round(avg(s.salary)) as salary
            from gm_vacancies as v
            join avg_sal_gm as s on s.id = v.id
            group by v.english_lvl
            order by salary desc;

    --6. Top fields/specialisation by salary
        create materialized view top_fields as
            with hh as (
                select
                    r.name as name,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from hh_vacancies as v
                join hh_roles as r on r.id = v.role_id
                left join avg_sal_hh as s on s.id = v.id
                group by r.name
            ), gj as (
                select
                    f.name as name,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from gj_fields as f
                left join avg_sal_gj as s on s.id = f.id
                group by f.name
            ) select
                name,
                sum(count) as count,
                round(avg(salary)) as salary
            from (
                select * from hh 
                union all
                select * from gj 
            )
            group by name
            order by count desc
            limit 100;

    --7. Top grade by salary
        create materialized view top_grade as
            select
                l.name,
                count(*) as count,
                round(avg(s.salary)) as salary
            from gj_level as l
            left join avg_sal_gj as s on s.id = l.id
            group by l.name
            order by count desc;

    --8. Top experience by salary
        create materialized view top_experience as
            with hh as (
                select
                    e.name,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from hh_vacancies as v
                join hh_experience as e on e.id = v.experience_id
                left join avg_sal_hh as s on s.id = v.id
                group by e.name
            ), gj as (
                select
                    case 
                        when v.experience like '%От%3%до%' then 'От 3 до 6 лет'
                        when v.experience like '%Более%5%' then 'Более 6 лет'
                        else v.experience
                    end as experience,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from gj_vacancies as v
                left join avg_sal_gj as s on s.id = v.id
                group by v.experience
            ) select
                name,
                sum(count) as count,
                round(avg(salary)) as salary
            from (
                select * from hh
                union all
                select * from gj
            )
            group by name
            order by count desc;

    --9. Top employment
        create materialized view top_employment as
            select 
                e.name,
                count(*) as count,
                round(avg(s.salary)) as salary
            from hh_vacancies as v
            join hh_employment as e on e.id = v.employment_id
            left join avg_sal_hh as s on s.id = v.id
            group by e.name
            order by count desc;

    --10. Top schedule
        create materialized view top_schedule as
            with hh as (
                select 
                    sc.name as name,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from hh_vacancies as v
                join hh_schedule as sc on sc.id = v.schedule_id
                left join avg_sal_hh as s on s.id = v.id
                group by sc.name
            ), gj as (
                select
                    sc.name as name,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from gj_vacancies as v
                join gj_jobformat as sc on sc.id = v.id
                left join avg_sal_gj as s on s.id = v.id
                group by sc.name
            ), gm as (
                with vacs as (
                    select 
                        v.id,
                        name_op as op
                    from gm_vacancies as v
                    cross join lateral (
                        values 
                            (case when remote_op is not null then 'Удаленная работа' end),
                            (case when office_op is not null then 'Работа в офисе' end)
                    ) as t(name_op)
                    where (remote_op is not null or office_op is not null)
                    and name_op is not null
                ) select
                    v.op as name,
                    count(*) as count,
                    round(avg(s.salary)) as salary
                from vacs as v
                left join avg_sal_gm as s on s.id = v.id
                group by v.op
            ) select 
                v.name,
                sum(count) as count,
                round(avg(v.salary)) as salary
            from (
                select * from hh
                union all
                select * from gj
                union all
                select * from gm
            ) as v
            group by v.name
            order by count desc;

    --11. Vacs count per day
        create materialized view vacs_pday as
            with hh as (
                select 
                    date(v.publish_date) as date,
                    count(*)
                from hh_vacancies as v
                group by date(v.publish_date)
            ), gm as (
               select 
                    date(v.publish_date) as date,
                    count(*)
                from gm_vacancies as v
                group by date(v.publish_date) 
            ), gj as (
               select 
                    date(v.publish_date) as date,
                    count(*)
                from gj_vacancies as v
                group by date(v.publish_date) 
            ) select
                date,
                sum(count) as count
            from (
                select * from hh
                union all
                select * from gj
                union all
                select * from gm
            )
            group by date
            order by date desc;

    --. Salary by mounths, years
--//