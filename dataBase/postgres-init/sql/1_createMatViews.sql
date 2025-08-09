--// Aggregate data level 1
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


--// Aggregate data level 2
    
    --1 Grade counts with salary per months
        create materialized view grades_count_sal_pmonths as 
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
            ) as data
            group by date_trunc('month', date), grade;

    --2 Skills counts with salary per months
        create materialized view skills_count_sal as
            select
                id,
                name as skill,
                salary,
                date_trunc('month', date) as date
            from (
                select 
                    cast(sk.id as text) as id, 
                    sk.name,
                    s.salary,
                    v.publish_date as date
                from hh_skills as sk
                join hh_vacancies as v on v.id = sk.id
                left join avg_sal_hh as s on sk.id = s.id

                union all

                select
                    sk.id,
                    sk.name,
                    s.salary,
                    v.publish_date as date
                from gj_skills as sk
                join gj_vacancies as v on v.id = sk.id
                left join avg_sal_gj as s on sk.id = s.id

                union all

                select
                    cast(sk.id as text) as id, 
                    sk.name,
                    s.salary,
                    v.publish_date as date
                from gm_skills as sk
                join gm_vacancies as v on v.id = sk.id
                left join avg_sal_gm as s on sk.id = s.id
            ) as data;

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
                    ) as data
                ), skills as (
                    select
                        count(distinct name) as res
                    from (
                        select distinct name from hh_skills
                        union all
                        select distinct name from gj_skills
                        union all
                        select distinct name from gm_skills
                    ) as data
                ), companies as (
                    select
                        count(distinct employer) as res
                    from (
                        select name as employer from hh_employers
                        union all
                        select distinct employer from gm_vacancies
                        union all
                        select distinct employer from gj_vacancies
                    ) as data
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
                ) as data;
    
        --3. Vacs count per day/
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
    --//


    --// Grades
        --1. Top grade
            create materialized view top_grades as           
                select
                    grade,
                    sum(count) as count,
                    round(avg(average)) as salary
                from grades_count_sal_pmonths
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
                from grades_count_sal_pmonths
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
                from grades_count_sal_pmonths
                group by date
                order by date desc;
    --//


    --// Skills
        --1. Top skills
            create materialized view top_skills as
                select
                    skill,
                    count(*),
                    round(avg(salary)) as salary
                from skills_count_sal
                group by skill
                order by count desc
                limit 100;
    
        --2. Combined skills by 2
            create materialized view top_combined_skills_by2 as
                with pairs as (
                    select 
                        f.skill as first,
                        s.skill as second,
                        f.salary as salary
                    from skills_count_sal as f
                    join skills_count_sal as s on f.id = s.id and f.skill < s.skill
                ) select
                    first,
                    second,
                    count(*) as count,
                    round(avg(salary)) as salary
                from pairs
                group by first, second
                order by count desc
                limit 15;

        --3. Combined skills by 3
            create materialized view top_combined_skills_by3 as
                with skills as (
                    select 
                        f.id,
                        f.skill as first,
                        s.skill as second,
                        t.skill as third,
                        f.salary as salary
                    from skills_count_sal as f
                    join skills_count_sal as s on f.id = s.id and f.skill < s.skill
                    join skills_count_sal as t on f.id = t.id and s.skill < t.skill
                ) select
                    first,
                    second,
                    third,
                    count(*) as count,
                    round(avg(salary)) as salary
                from skills
                group by first, second, third
                order by count desc
                limit 15;
    
        --4. Top skills for grades
            create materialized view top_skills_by_grades as
                with skill_counts as (
                    select 
                        g.grade,
                        s.skill,
                        count(*) as skill_count
                    from (
                        select
                            id,
                            name as grade
                        from gj_level

                        union all

                        select
                            cast(id as text) as id,
                            grade
                        from grades_hh

                        union all
                        select
                            cast(id as text) as id,
                            grade
                        from grades_gm
                    ) as g
                    join skills_count_sal s on g.id = s.id
                    where g.grade in ('Джуниор', 'Миддл', 'Сеньор', 'Стажер')
                    group by g.grade, s.skill
                ),
                ranked_skills as (
                    select 
                        grade,
                        skill,
                        skill_count,
                        row_number() over (partition by grade order by skill_count desc) as skill_rank
                    from skill_counts
                )
                select 
                    grade,
                    max(case when skill_rank = 1 then skill end) as fn,
                    max(case when skill_rank = 1 then skill_count end) as fc,
                    max(case when skill_rank = 2 then skill end) as sn,
                    max(case when skill_rank = 2 then skill_count end) as sc,
                    max(case when skill_rank = 3 then skill end) as tn,
                    max(case when skill_rank = 3 then skill_count end) as tc,
                    max(case when skill_rank = 4 then skill end) as fon,
                    max(case when skill_rank = 4 then skill_count end) as foc
                from ranked_skills
                where skill_rank <= 4
                group by grade;

        --5. Top skills for fields
            create materialized view top_skills_by_fields as
                with fields as (
                    select
                        id,
                        field
                    from (
                        select
                            cast(v.id as text) as id,
                            r.name as field
                        from hh_vacancies as v
                        join hh_roles as r on r.id = v.role_id

                        union all

                        select
                            id,
                            name as field
                        from gj_fields
                    ) as data
                ), skills as (
                    select
                        f.field,
                        s.skill,
                        count(*) as count
                    from fields as f
                    join skills_count_sal as s on s.id = f.id
                    group by f.field, s.skill
                ), top_fields as (
                    select
                        field,
                        sum(count) as count
                    from skills
                    group by field
                    order by count desc
                    limit 15
                ), ranked_skills as (
                    select 
                        field,
                        skill,
                        count,
                        row_number() over (partition by field order by count desc) as rank
                    from skills
                    where field in (select field from top_fields)
                ) select 
                    field,
                    max(case when rank = 1 then skill end) as fn,
                    max(case when rank = 1 then count end) as fc,
                    max(case when rank = 2 then skill end) as sn,
                    max(case when rank = 2 then count end) as sc,
                    max(case when rank = 3 then skill end) as tn,
                    max(case when rank = 3 then count end) as tc,
                    max(case when rank = 4 then skill end) as fon,
                    max(case when rank = 4 then count end) as foc
                from ranked_skills
                where rank <= 4
                group by field;
    --//


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
            ) as data
            where count > 5
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
            limit 100;

    

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