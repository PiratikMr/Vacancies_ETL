--// Agregate data level 1
    --1 Transformed hh salaries
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

    --2 Transformed gj salaries
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

    --3 Transformed gm salaries
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

    --4 Extracted hh grades
        create materialized view grades_hh as
            with grade_flags as (
                select
                    id,
                    lower(name) ~ '.*(jun|джун).*' as jun,
                    lower(name) ~ '.*(midd|мидд).*' as midd,
                    lower(name) ~ '.*(sen|сеньор).*' as sen,
                    lower(name) ~ '.*(intern|стаж).*' as intern
                from hh_vacancies
            ) select
                id,
                unnest(array_remove(array[
                    case when jun then 'Джуниор' end,
                    case when midd then 'Миддл' end,
                    case when sen then 'Сеньор' end,
                    case when intern then 'Стажер' end
                ], null)) as grade
            from grade_flags;
        
    --5 Extracted gm grades
        create materialized view grades_gm as
            with grade_flags as (
                select
                    id,
                    lower(name) ~ '.*(jun|джун).*' as jun,
                    lower(name) ~ '.*(midd|мидд).*' as midd,
                    lower(name) ~ '.*(sen|сеньор).*' as sen,
                    lower(name) ~ '.*(intern|стаж).*' as intern
                from gm_vacancies
            ) select
                id,
                unnest(array_remove(array[
                    case when jun then 'Джуниор' end,
                    case when midd then 'Миддл' end,
                    case when sen then 'Сеньор' end,
                    case when intern then 'Стажер' end
                ], null)) as grade
            from grade_flags;
--//


--// Agregate data level 2
    
    --1 Grade vacs with salary per month
        create materialized view grades_vacs_pmonths as 
            select
                grade,
                date_trunc('month', date) as date,
                avg(salary) as average,
                count(*) as count
            from (
                select 
                    g.grade as grade,
                    v.publish_date as date,
                    s.salary
                from grades_hh as g
                join hh_vacancies as v on v.id = g.id
                left join avg_sal_hh as s on s.id = v.id

                union all

                select 
                    g.grade  as grade,
                    v.publish_date as date,
                    s.salary
                from grades_gm as g
                join gm_vacancies as v on v.id = g.id
                left join avg_sal_gm as s on s.id = v.id

                union all

                select 
                    g.name as grade,
                    v.publish_date as date,
                    s.salary
                from gj_level as g
                join gj_vacancies as v on v.id = g.id
                left join avg_sal_gj as s on s.id = v.id
            )
            group by date_trunc('month', date), grade;
--//


--// Final data

    --// Number values
        --1. Number of vacancies
            create materialized view vacs_count as
                with counts as (
                    select
                        count(*) as total,
                        count(case when site = 'hh' then 1 end) as hh_count,
                        count(case when site = 'gm' then 1 end) as gm_count,
                        count(case when site = 'gj' then 1 end) as gj_count,
                        count(case when salary_from is not null and salary_to is not null then 1 end) as range_count,
                        count(case when salary_from is not null or salary_to is not null then 1 end) as salary_count,
                        count(case when currency_id in ('USD', 'EUR') then 1 end) ed_count
                    from (
                        select 'hh' as site, cast(id as text), salary_from, salary_to, currency_id from hh_vacancies
                        union all
                        select 'gm' as site, cast(id as text), salary_from, salary_to, currency_id from gm_vacancies
                        union all
                        select 'gj' as site, id, salary_from, salary_to, currency_id from gj_vacancies
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
                    c.hh_count as hh,
                    c.gm_count as gm,
                    c.gj_count as gj,
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
                select 
                    round(avg(salary)) as average,
                    round(cast(percentile_cont(0.5) within group (order by salary) as numeric)) as mediane,
                    round(min(salary)) as min,
                    round(max(salary)) as max
                from (
                    select salary from avg_sal_hh
                    union all
                    select salary from avg_sal_gj
                    union all
                    select salary from avg_sal_gm
                );

    --// Grades
        --1. Top grade by salary
            create materialized view top_grades as           
                select
                    grade,
                    sum(count) as count,
                    round(avg(average)) as salary
                from grades_vacs_pmonths
                group by grade
                order by count desc;

        --2. Grade vacancies count by months
            create materialized view vacs_grade_pmonths as
                select
                    date,
                    sum(count) filter (where grade = 'Джуниор') as junior,
                    sum(count) filter (where grade = 'Миддл') as middle,
                    sum(count) filter (where grade = 'Сеньор') as senior,
                    sum(count) filter (where grade = 'Стажер') as intern
                from grades_vacs_pmonths
                group by date
                order by date desc;

        --3. Grade average salary by months
            create materialized view sal_grades_pmonths as
                select
                    date,
                    avg(average) filter (where grade = 'Джуниор') as junior,
                    avg(average) filter (where grade = 'Миддл') as middle,
                    avg(average) filter (where grade = 'Сеньор') as senior,
                    avg(average) filter (where grade = 'Стажер') as intern
                from grades_vacs_pmonths
                group by date
                order by date desc;


    --3. Top companies by salary
        create materialized view top_companies as
            with vacs as (
                select
                    e.name as name,
                    s.salary as salary
                from hh_vacancies as v
                join hh_employers as e on e.id = v.employer_id
                left join avg_sal_hh as s on s.id = v.id

                union all

                select 
                    v.employer as name,
                    s.salary as salary
                from gj_vacancies as v
                left join avg_sal_gj as s on s.id = v.id
                where v.employer is not null

                union all

                select 
                    v.employer as name,
                    s.salary as salary
                from gm_vacancies as v
                left join avg_sal_gm as s on s.id = v.id
                where v.employer is not null
            ) select
                name,
                count,
                salary
            from (
                select 
                    name,
                    count(*) as count,
                    round(avg(salary)) as salary
                from vacs
                group by name
            )
            where count > 5
            order by count desc
            limit 100;
            
    --4. Top skills, mediane salary for each
        create materialized view top_skills as
            with vacs as (
                select 
                    sk.name,
                    s.salary as salary
                from hh_skills as sk
                left join avg_sal_hh as s on s.id = sk.id

                union all

                select 
                    sk.name,
                    s.salary as salary
                from gj_skills as sk
                left join avg_sal_gj as s on s.id = sk.id

                union all

                select 
                    sk.name,
                    s.salary as salary
                from gm_skills as sk
                left join avg_sal_gm as s on s.id = sk.id
            ) select
                case when name is null then 'Без навыка' else name end as name,
                count(*) as count,
                round(avg(salary)) as salary
            from vacs
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
            with vacs as (
                select
                    r.name as name,
                    s.salary as salary
                from hh_vacancies as v
                join hh_roles as r on r.id = v.role_id
                left join avg_sal_hh as s on s.id = v.id

                union all

                select
                    f.name as name,
                    s.salary as salary
                from gj_fields as f
                left join avg_sal_gj as s on s.id = f.id

                union all

                select
                    f.name as name,
                    s.salary as salary
                from gj_fields as f
                left join avg_sal_gj as s on s.id = f.id
            ) select
                name,
                count(*) as count,
                round(avg(salary)) as salary
            from vacs
            group by name
            order by count desc
            limit 100;s

    

    --8. Top experience by salary
        create materialized view top_experiences as
            with vacs as (
                select
                    e.name as name,
                    s.salary as salary
                from hh_vacancies as v
                join hh_experience as e on e.id = v.experience_id
                left join avg_sal_hh as s on s.id = v.id

                union all

                select
                    case 
                        when v.experience like '%От%3%до%' then 'От 3 до 6 лет'
                        when v.experience like '%Более%5%' then 'Более 6 лет'
                        else v.experience
                    end as name,
                    s.salary as salary
                from gj_vacancies as v
                left join avg_sal_gj as s on s.id = v.id
            ) select
                name,
                count(*) as count,
                round(avg(salary)) as salary
            from vacs
            group by name
            order by count desc;

    --9. Top employment
        create materialized view top_employments as
            with vacs as (

                select 
                    e.name,
                    s.salary as salary
                from hh_vacancies as v
                join hh_employment as e on e.id = v.employment_id
                left join avg_sal_hh as s on s.id = v.id 

            ) select 
                name,
                count(*) as count,
                round(avg(salary)) as salary
            from vacs
            group by name
            order by count desc;

    --10. Top schedule
        create materialized view top_schedules as
            with vacs as (
                select 
                    sc.name as name,
                    s.salary as salary
                from hh_vacancies as v
                join hh_schedule as sc on sc.id = v.schedule_id
                left join avg_sal_hh as s on s.id = v.id

                union all

                select
                    sc.name as name,
                    s.salary as salary
                from gj_vacancies as v
                join gj_jobformat as sc on sc.id = v.id
                left join avg_sal_gj as s on s.id = v.id
                
                union all

                (with v as (
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
                    s.salary as salary
                from v
                left join avg_sal_gm as s on s.id = v.id)
            ) select
                name,
                count(*) as count,
                round(avg(salary)) as salary
            from vacs
            group by name
            order by count desc;           

    --11. Vacs count per day
        create materialized view vacs_pday as
            with vacs as (
                select 
                    date(publish_date) as date
                from hh_vacancies

                union all

                select 
                    date(publish_date) as date
                from gj_vacancies

                union all

                select 
                    date(publish_date) as date
                from gm_vacancies
            ) select
                date,
                count(*) as count
            from vacs
            group by date
            order by date desc;

    --12. Average salary per quarter yaer
        create materialized view sal_pquarters as
            with vacs as (
                select
                   to_char(date_trunc('quarter', publish_date), 'YYYY-MM') as date,
                   salary
                from (
                    select v.publish_date, s.salary from hh_vacancies as v
                    left join avg_sal_hh as s on s.id = v.id

                    union all

                    select v.publish_date, s.salary from gj_vacancies as v
                    left join avg_sal_gj as s on s.id = v.id

                    union all

                    select v.publish_date, s.salary from gm_vacancies as v
                    left join avg_sal_gm as s on s.id = v.id
                ) as v
            ) select
                date,
                round(avg(salary)) as salary
            from vacs
            group by date
            order by date;

--//